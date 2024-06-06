from __future__ import annotations

import logging
import traceback
import uuid
from typing import TYPE_CHECKING

from org.somda.protosdc.proto.model import sdc_messages_pb2
from org.somda.protosdc.proto.model import sdc_services_pb2_grpc
from org.somda.protosdc.proto.model.biceps.localizedtext_pb2 import LocalizedTextMsg
from sdc11073.definitions_sdc import SdcV1Definitions
from sdc11073.xml_types import msg_types

from pyprotosdc.mapping.basic_mappers import (enum_attr_to_p,
                                              string_value_to_p,
                                              decimal_from_p)
from pyprotosdc.mapping.mapping_helpers import attr_name_to_p, get_p_attr
from pyprotosdc.mapping.msgtypes_mappers import set_mdib_version_group

if TYPE_CHECKING:
    from enum import Enum
    from pyprotosdc.provider.provider import GSdcDevice
    from sdc11073.provider.operations import OperationDefinitionProtocol
    from sdc11073.mdib.mdibbase import MdibVersionGroup


def set_method(response):
    """Decorator for all set methods .

    If handles the case that there is no registered operation for the operation handle.
    """

    def inner_decorator(f):
        def wrapped(self, request, context):
            try:
                transaction_id = self._provider.generate_transaction_id()
                invocation_info = response.payload.abstract_set_response.invocation_info
                invocation_info.transaction_id.unsigned_int = transaction_id
                op_handle = request.payload.abstract_set.operation_handle_ref.string
                operation = self._provider.get_operation_by_handle(op_handle)
                response.addressing.relates_id.value = request.addressing.message_id
                response.addressing.message_id = uuid.uuid4().urn
                if operation is not None:
                    return f(self, operation, transaction_id, response, request, context)
                else:
                    enum_attr_to_p(msg_types.InvocationState.FAILED, invocation_info.invocation_state)
                    enum_attr_to_p(msg_types.InvocationError.INVALID_VALUE, invocation_info.invocation_error)
                    error_text = LocalizedTextMsg()
                    error_text.localized_text_content.string = f'no handler registered for "{op_handle}"'
                    string_value_to_p('EN_en', getattr(error_text, attr_name_to_p('Lang')))
                    invocation_info.invocation_error_message.append(error_text)
                    self._logger.warning('operation request "%s" failed, transaction id = %d, invocation-state=%r',
                                         op_handle, transaction_id, invocation_info.invocation_state.FAIL)
                    return response
            except Exception:
                print(traceback.format_exc())
                raise

        return wrapped

    return inner_decorator


class SetService(sdc_services_pb2_grpc.SetServiceServicer):
    def __init__(self, provider: GSdcDevice):
        super().__init__()
        self._provider = provider
        self._mdib = provider.mdib
        self._logger = logging.getLogger('sdc.grpc.dev.SetService')

    @set_method(sdc_messages_pb2.ActivateResponse())
    def Activate(self,
                 operation,
                 transaction_id,
                 response: sdc_messages_pb2.ActivateResponse,
                 request: sdc_messages_pb2.ActivateRequest,
                 context) -> sdc_messages_pb2.ActivateResponse:

        # convert proto arguments to pm arguments
        pm_activate = msg_types.Activate()
        for arg in request.payload.argument:
            pm_activate.add_argument(arg.arg_value)
        pm_invocation_state = self._provider.handle_operation_request(operation,
                                                                      request,
                                                                      pm_activate,
                                                                      transaction_id)
        self._logger.info('activate operation request "%s" handled, transaction id = %d, invocation-state=%r',
                          operation.handle, transaction_id, pm_invocation_state)
        invocation_info = response.payload.abstract_set_response.invocation_info
        enum_attr_to_p(pm_invocation_state.value, invocation_info.invocation_state)
        response.addressing.action = SdcV1Definitions.Actions.ActivateResponse

        return response

    @set_method(sdc_messages_pb2.SetMetricStateResponse())
    def SetMetricState(self,
                       operation,
                       transaction_id,
                       response: sdc_messages_pb2.SetMetricStateResponse,
                       request: sdc_messages_pb2.SetMetricStateRequest,
                       context) -> sdc_messages_pb2.SetMetricStateResponse:
        args = list(request.payload.proposed_metric_state)
        proposed_states = self._provider.msg_reader.read_states(args, self._mdib)
        pm_argument = msg_types.SetMetricState()
        pm_argument.ProposedMetricState.extend(proposed_states)
        pm_invocation_state = self._provider.handle_operation_request(operation,
                                                                      request,
                                                                      pm_argument,
                                                                      transaction_id)
        self._logger.info('operation request "%s" handled, transaction id = %d, invocation-state=%r',
                          operation.handle, transaction_id, pm_invocation_state)
        invocation_info = response.payload.abstract_set_response.invocation_info
        enum_attr_to_p(pm_invocation_state.value, invocation_info.invocation_state)
        mdib_version_group_msg = get_p_attr(response.payload.abstract_set_response, 'MdibVersionGroup')
        set_mdib_version_group(mdib_version_group_msg, self._mdib.mdib_version_group)

        self._logger.debug('SetMetricState called, transaction %d', transaction_id)
        return response

    @set_method(sdc_messages_pb2.SetComponentStateResponse())
    def SetComponentState(self,
                          operation,
                          transaction_id,
                          response: sdc_messages_pb2.SetComponentStateResponse,
                          request: sdc_messages_pb2.SetComponentStateRequest,
                          context) -> sdc_messages_pb2.SetComponentStateResponse:
        args = list(request.payload.proposed_component_state)
        proposed_states = self._provider.msg_reader.read_states(args, self._mdib)
        pm_argument = msg_types.SetComponentState()
        pm_argument.ProposedComponentState.extend(proposed_states)
        pm_invocation_state = self._provider.handle_operation_request(operation,
                                                                      request,
                                                                      pm_argument,
                                                                      transaction_id)
        self._logger.info('operation request "%s" handled, transaction id = %d, invocation-state=%r',
                          operation.handle, transaction_id, pm_invocation_state)
        invocation_info = response.payload.abstract_set_response.invocation_info
        enum_attr_to_p(pm_invocation_state.value, invocation_info.invocation_state)

        mdib_version_group_msg = get_p_attr(response.payload.abstract_set_response, 'MdibVersionGroup')
        set_mdib_version_group(mdib_version_group_msg, self._mdib.mdib_version_group)

        self._logger.debug('SetComponentState called, transaction %d', transaction_id)
        return response

    @set_method(sdc_messages_pb2.SetContextStateResponse())
    def SetContextState(self,
                        operation,
                        transaction_id,
                        response: sdc_messages_pb2.SetContextStateResponse,
                        request: sdc_messages_pb2.SetContextStateRequest,
                        context) -> sdc_messages_pb2.SetContextStateResponse:
        args = list(request.payload.proposed_context_state)
        proposed_states = self._provider.msg_reader.read_states(args, self._mdib)
        pm_argument = msg_types.SetComponentState()
        pm_argument.ProposedComponentState.extend(proposed_states)
        pm_invocation_state = self._provider.handle_operation_request(operation,
                                                                      request,
                                                                      pm_argument,
                                                                      transaction_id)
        self._logger.info('operation request "%s" handled, transaction id = %d, invocation-state=%r',
                          operation.handle, transaction_id, pm_invocation_state)
        invocation_info = response.payload.abstract_set_response.invocation_info
        enum_attr_to_p(pm_invocation_state.value, invocation_info.invocation_state)

        mdib_version_group_msg = get_p_attr(response.payload.abstract_set_response, 'MdibVersionGroup')
        set_mdib_version_group(mdib_version_group_msg, self._mdib.mdib_version_group)

        self._logger.debug('SetContextState called, transaction %d', transaction_id)
        return response

    @set_method(sdc_messages_pb2.SetAlertStateResponse())
    def SetAlertState(self,
                      operation,
                      transaction_id,
                      response: sdc_messages_pb2.SetAlertStateResponse,
                      request: sdc_messages_pb2.SetAlertStateRequest,
                      context) -> sdc_messages_pb2.SetAlertStateResponse:
        args = list(request.payload.proposed_component_state)
        proposed_states = self._provider.msg_reader.read_states(args, self._mdib)
        pm_argument = msg_types.SetComponentState()
        pm_argument.ProposedComponentState.extend(proposed_states)
        pm_invocation_state = self._provider.handle_operation_request(operation,
                                                                      request,
                                                                      pm_argument,
                                                                      transaction_id)
        self._logger.info('operation request "%s" handled, transaction id = %d, invocation-state=%r',
                          operation.handle, transaction_id, pm_invocation_state)
        invocation_info = response.payload.abstract_set_response.invocation_info
        enum_attr_to_p(pm_invocation_state.value, invocation_info.invocation_state)

        mdib_version_group_msg = get_p_attr(response.payload.abstract_set_response, 'MdibVersionGroup')
        set_mdib_version_group(mdib_version_group_msg, self._mdib.mdib_version_group)

        self._logger.debug('SetAlertState called, transaction %d', transaction_id)
        return response

    @set_method(sdc_messages_pb2.SetStringResponse())
    def SetString(self,
                  operation,
                  transaction_id,
                  response: sdc_messages_pb2.SetStringResponse,
                  request: sdc_messages_pb2.SetStringRequest,
                  context) -> sdc_messages_pb2.SetStringResponse:
        value = request.payload.requested_string_value
        pm_argument = msg_types.SetString()
        pm_argument.RequestedStringValue = value
        pm_invocation_state = self._provider.handle_operation_request(operation,
                                                                      request,
                                                                      pm_argument,
                                                                      transaction_id)
        invocation_info = response.payload.abstract_set_response.invocation_info
        enum_attr_to_p(pm_invocation_state.value, invocation_info.invocation_state)

        mdib_version_group_msg = get_p_attr(response.payload.abstract_set_response, 'MdibVersionGroup')
        set_mdib_version_group(mdib_version_group_msg, self._mdib.mdib_version_group)

        self._logger.debug('SetString called, transaction %d', transaction_id)
        return response

    @set_method(sdc_messages_pb2.SetValueResponse())
    def SetValue(self,
                 operation,
                 transaction_id,
                 response: sdc_messages_pb2.SetValueResponse,
                 request: sdc_messages_pb2.SetValueRequest,
                 context) -> sdc_messages_pb2.SetValueResponse:

        p_value = request.payload.requested_numeric_value
        pm_value = decimal_from_p(p_value)
        pm_argument = msg_types.SetValue()
        pm_argument.RequestedNumericValue = pm_value
        pm_invocation_state = self._provider.handle_operation_request(operation,
                                                                      request,
                                                                      pm_argument,
                                                                      transaction_id)
        invocation_info = response.payload.abstract_set_response.invocation_info

        enum_attr_to_p(pm_invocation_state.value, invocation_info.invocation_state)

        mdib_version_group_msg = get_p_attr(response.payload.abstract_set_response, 'MdibVersionGroup')
        set_mdib_version_group(mdib_version_group_msg, self._mdib.mdib_version_group)

        self._logger.debug('SetValue called, transaction %d state = %s',
                           transaction_id, pm_invocation_state)
        return response

    def OperationInvokedReport(self, request, context):
        actions = [SdcV1Definitions.Actions.OperationInvokedReport]
        self._logger.debug('OperationInvokedReport called')
        subscription = self._provider.subscriptions_manager.on_subscribe_request(actions)
        _run = True
        while _run:
            report = subscription.reports.get()
            if report == 'stop':
                _run = False
                self._logger.info('OperationInvokedReport stopped')
            else:
                self._logger.info('yield OperationInvokedReport %s', report.__class__.__name__)
                yield report
                # self._logger.info('yield OperationInvokedReport done')
