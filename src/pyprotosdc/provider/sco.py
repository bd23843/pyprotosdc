from __future__ import annotations
from typing import TYPE_CHECKING, Any
from sdc11073 import loghelper
from sdc11073.provider.sco import _OperationsWorker

if TYPE_CHECKING:
    from enum import Enum
    from sdc11073.mdib.descriptorcontainers import AbstractDescriptorProtocol
    from sdc11073.roles.providerbase import OperationClassGetter
    from sdc11073.provider.porttypes.setserviceimpl import SetServiceProtocol
    from sdc11073.mdib.providermdib import ProviderMdib
    from sdc11073.provider.sco import OperationDefinitionBase

class ScoOperationsRegistry:

    def __init__(self, set_service: SetServiceProtocol,
                 operation_cls_getter: OperationClassGetter,
                 mdib: ProviderMdib,
                 sco_descriptor_container: AbstractDescriptorProtocol,
                 log_prefix: str | None = None):
        self._worker = None
        self._set_service: SetServiceProtocol = set_service
        self.operation_cls_getter = operation_cls_getter
        self._mdib = mdib
        self.sco_descriptor_container = sco_descriptor_container
        self._log_prefix = log_prefix
        self._logger = loghelper.get_logger_adapter('sdc.device.op_reg', log_prefix)
        self._registered_operations = {}  # lookup by handle

    def check_invocation_timeouts(self):
        """Call check_timeout of all registered operations."""
        for op in self._registered_operations.values():
            op.check_timeout()

    def register_operation(self, operation: OperationDefinitionBase):
        """Register the operation."""
        if operation.handle in self._registered_operations:
            self._logger.debug('handle %s is already registered, will re-use it', operation.handle)
        operation.set_mdib(self._mdib, self.sco_descriptor_container.Handle)
        self._logger.info('register operation "%s"', operation)
        self._registered_operations[operation.handle] = operation

    def unregister_operation_by_handle(self, operation_handle: str):
        """Un-register the operation."""
        del self._registered_operations[operation_handle]

    def get_operation_by_handle(self, operation_handle: str) -> OperationDefinitionBase:
        """Get OperationDefinition for given handle."""
        return self._registered_operations.get(operation_handle)

    def handle_operation_request(self,
                                 operation: OperationDefinitionBase,
                                 request: Any,
                                 operation_request: Any,
                                 transaction_id: int) -> Enum:
        """Handle operation immediately or delayed in worker thread, depending on operation.delayed_processing."""
        InvocationState = self._mdib.data_model.msg_types.InvocationState  # noqa: N806

        if operation.delayed_processing:
            self._worker.enqueue_operation(operation, request, operation_request, transaction_id)
            return InvocationState.WAIT
        try:
            execute_result = operation.execute_operation(request, operation_request)
            self._logger.info('%s: successfully finished operation "%s"', operation.__class__.__name__,
                              operation.handle)
            self._set_service.notify_operation(operation, transaction_id, execute_result.invocation_state,
                                               self._mdib.mdib_version_group, execute_result.operation_target_handle)
            self._logger.debug('notifications for operation %s sent', operation.handle)
            return InvocationState.FINISHED
        except Exception as ex:
            self._logger.error('%s: error executing operation "%s": %s', operation.__class__.__name__,
                               operation.handle, traceback.format_exc())
            self._set_service.notify_operation(
                operation, transaction_id, InvocationState.FAILED, self._mdib.mdib_version_group,
                error=self._mdib.data_model.msg_types.InvocationError.OTHER, error_message=repr(ex))
            return InvocationState.FAILED

    def start_worker(self):
        """Start worker thread."""
        if self._worker is not None:
            raise ApiUsageError('SCO worker is already running')
        self._worker = _OperationsWorker(self, self._set_service, self._mdib, self._log_prefix)
        self._worker.start()

    def stop_worker(self):
        """Stop worker thread."""
        if self._worker is not None:
            self._worker.stop()
            self._worker = None
