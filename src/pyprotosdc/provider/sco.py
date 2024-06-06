from __future__ import annotations

import queue
import threading
import time
import traceback
from dataclasses import dataclass
from typing import TYPE_CHECKING

from sdc11073 import loghelper
from sdc11073.exceptions import ApiUsageError
from sdc11073.xml_types import msg_types

if TYPE_CHECKING:
    from sdc11073.mdib.descriptorcontainers import AbstractDescriptorProtocol
    from sdc11073.roles.providerbase import OperationClassGetter
    from sdc11073.mdib.providermdib import ProviderMdib
    from sdc11073.provider.sco import OperationDefinitionBase
    from .subscriptionmgr import GSubscriptionsManager
    from pyprotosdc.mapping.msgtypes_mappers import AnySetServiceRequest


@dataclass
class _EnqueuedOperation:
    transaction_id: int
    operation: OperationDefinitionBase | None
    request: AnySetServiceRequest | None  # protobuf request
    operation_request: msg_types.AbstractSet | None  # request converted to corresponding msg_types instance


stop_msg = _EnqueuedOperation(-1, None, None, None)  # special instance that is uses as stop message


class _OperationsWorker(threading.Thread):
    """Thread that enqueues and processes all operations.

    Progress notifications are sent via subscription manager.
    """

    def __init__(self,
                 operations_registry: ScoOperationsRegistry,
                 subscriptions_manager: GSubscriptionsManager,
                 mdib: ProviderMdib,
                 log_prefix: str):
        super().__init__(name='DeviceOperationsWorker')
        self.daemon = True
        self._operations_registry = operations_registry
        self._subscriptions_manager = subscriptions_manager
        self._mdib = mdib
        self._operations_queue = queue.Queue(10)  # spooled operations
        self._logger = loghelper.get_logger_adapter('sdc.device.op_worker', log_prefix)

    def enqueue_operation(self, operation: OperationDefinitionBase,
                          request: AnySetServiceRequest,  # protobuf request
                          operation_request: msg_types.AbstractSet,  # request converted to msg_types
                          transaction_id: int):
        """Enqueue operation."""
        self._operations_queue.put(_EnqueuedOperation(transaction_id, operation, request, operation_request), timeout=1)

    def run(self):
        while True:
            try:
                try:
                    from_queue: _EnqueuedOperation = self._operations_queue.get(timeout=1.0)
                except queue.Empty:
                    self._operations_registry.check_invocation_timeouts()
                else:
                    if from_queue.transaction_id == -1:
                        self._logger.info('stop request found. Terminating now.')
                        return
                    # tr_id, operation, request, operation_request = from_queue  # unpack tuple
                    time.sleep(0.001)
                    self._logger.info('%s: starting operation "%s" argument=%r',
                                      from_queue.operation.__class__.__name__,
                                      from_queue.operation.handle, from_queue.operation_request.argument)
                    # duplicate the WAIT response to the operation request as notification. Standard requires this.
                    self._subscriptions_manager.send_operation_invoked_report(
                        from_queue.operation, from_queue.transaction_id, msg_types.InvocationState.WAIT,
                        self._mdib.mdib_version_group)
                    time.sleep(0.001)  # not really necessary, but in real world there might also be some delay.
                    self._subscriptions_manager.send_operation_invoked_report(
                        from_queue.operation, from_queue.transaction_id, msg_types.InvocationState.START,
                        self._mdib.mdib_version_group)

                    try:
                        execute_result = from_queue.operation.execute_operation(from_queue.request,
                                                                                from_queue.operation_request)
                        self._logger.info('%s: successfully finished operation "%s"',
                                          from_queue.operation.__class__.__name__, from_queue.operation.handle)
                        self._subscriptions_manager.send_operation_invoked_report(
                            from_queue.operation, from_queue.transaction_id, execute_result.invocation_state,
                            self._mdib.mdib_version_group,
                            execute_result.operation_target_handle)

                    except Exception as ex:
                        self._logger.error('%s: error executing operation "%s": %s',
                                           from_queue.operation.__class__.__name__,
                                           from_queue.operation.handle, traceback.format_exc())
                        self._subscriptions_manager.send_operation_invoked_report(
                            from_queue.operation, from_queue.transaction_id, msg_types.InvocationState.FAILED,
                            self._mdib.mdib_version_group,
                            error=msg_types.InvocationError.OTHER, error_message=repr(ex))

            except Exception:
                self._logger.error('%s: unexpected error while handling operation: %s',
                                   self.__class__.__name__, traceback.format_exc())

    def stop(self):
        self._operations_queue.put(stop_msg)  # a dummy request to stop the thread
        self.join(timeout=1)


class ScoOperationsRegistry:

    def __init__(self,
                 subscriptions_manager: GSubscriptionsManager,
                 operation_cls_getter: OperationClassGetter,
                 mdib: ProviderMdib,
                 sco_descriptor_container: AbstractDescriptorProtocol,
                 log_prefix: str | None = None):
        self._worker = None
        self._subscriptions_manager = subscriptions_manager
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
                                 request: AnySetServiceRequest,
                                 converted_request: msg_types.AbstractSet,
                                 transaction_id: int) -> msg_types.InvocationState:
        """Handle operation immediately or delayed in worker thread, depending on operation.delayed_processing."""

        if operation.delayed_processing:
            self._worker.enqueue_operation(operation, request, converted_request, transaction_id)
            return msg_types.InvocationState.WAIT
        try:
            execute_result = operation.execute_operation(request, converted_request)
            self._logger.info('%s: successfully finished operation "%s"', operation.__class__.__name__,
                              operation.handle)
            self._subscriptions_manager.send_operation_invoked_report(
                operation, transaction_id, execute_result.invocation_state, self._mdib.mdib_version_group,
                execute_result.operation_target_handle)
            self._logger.debug('notifications for operation %s sent', operation.handle)
            return msg_types.InvocationState.FINISHED
        except Exception as ex:
            self._logger.error('%s: error executing operation "%s": %s', operation.__class__.__name__,
                               operation.handle, traceback.format_exc())
            self._subscriptions_manager.send_operation_invoked_report(
                operation, transaction_id, msg_types.InvocationState.FAILED, self._mdib.mdib_version_group,
                error=self._mdib.data_model.msg_types.InvocationError.OTHER, error_message=repr(ex))

            return msg_types.InvocationState.FAILED

    def start_worker(self):
        """Start worker thread."""
        if self._worker is not None:
            raise ApiUsageError('SCO worker is already running')
        self._worker = _OperationsWorker(self, self._subscriptions_manager, self._mdib, self._log_prefix)
        self._worker.start()

    def stop_worker(self):
        """Stop worker thread."""
        if self._worker is not None:
            self._worker.stop()
            self._worker = None
