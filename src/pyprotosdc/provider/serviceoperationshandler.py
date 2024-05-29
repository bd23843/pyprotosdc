from __future__ import annotations

from typing import TYPE_CHECKING

from org.somda.protosdc.proto.model.biceps.operationinvokedreport_pb2 import OperationInvokedReportMsg

if TYPE_CHECKING:
    from .provider import GSdcDevice
    from sdc11073.provider.sco import OperationDefinition
    from sdc11073.mdib.mdibbase import MdibVersionGroup


class ServiceOperationsHandler:

    def __init__(self, provider: GSdcDevice):
        self._provider = provider
        self._mdib = provider.mdib
        self._sdc_definitions = self._mdib.sdc_definitions


    # def _handle_operation_request(self, request_data: RequestData,
    #                               request: AbstractSet,
    #                               set_response: AbstractSetResponse) -> CreatedMessage:
    #     """Handle thew operation request by forwarding it to provider."""
    #     data_model = self._sdc_definitions.data_model
    #     operation = self._provider.get_operation_by_handle(request.OperationHandleRef)
    #     transaction_id = self._sdc_device.generate_transaction_id()
    #     set_response.InvocationInfo.TransactionId = transaction_id
    #     if operation is None:
    #         error_text = f'no handler registered for "{request.OperationHandleRef}"'
    #         self._logger.warning('handle operation request: {}', error_text)
    #         set_response.InvocationInfo.InvocationState = data_model.msg_types.InvocationState.FAILED
    #         set_response.InvocationInfo.InvocationError = data_model.msg_types.InvocationError.INVALID_VALUE
    #         set_response.InvocationInfo.add_error_message(error_text)
    #     else:
    #         invocation_state = self._sdc_device.handle_operation_request(operation,
    #                                                                      request_data.message_data.p_msg,
    #                                                                      request,
    #                                                                      transaction_id)
    #         self._logger.info('operation request "{}" handled, transaction id = {}, invocation-state={}',
    #                           request.OperationHandleRef, set_response.InvocationInfo.TransactionId, invocation_state)
    #         set_response.InvocationInfo.InvocationState = invocation_state
    #
    #     set_response.MdibVersion = self._mdib.mdib_version
    #     set_response.SequenceId = self._mdib.sequence_id
    #     set_response.InstanceId = self._mdib.instance_id
    #     return self._sdc_device.msg_factory.mk_reply_soap_message(request_data, set_response)

    def notify_operation(self,
                         operation: OperationDefinition,
                         transaction_id: int,
                         invocation_state: Enum,
                         mdib_version_group: MdibVersionGroup,
                         operation_target: str | None = None,
                         error: Enum | None = None,
                         error_message: str | None = None):
        msg = OperationInvokedReportMsg()
        self._provider.subscriptions_manager.send_operation_invoked_report
        pass
