from __future__ import annotations

import uuid
from typing import TYPE_CHECKING
import weakref
import threading
import logging
from org.somda.protosdc.proto.model import sdc_services_pb2_grpc, sdc_messages_pb2
from sdc11073 import  observableproperties
from sdc11073.definitions_sdc import SdcV1Definitions
from pyprotosdc.mapping.statesmapper import generic_state_to_p
from pyprotosdc.mapping.basic_mappers import decimal_to_p


if TYPE_CHECKING:
    from concurrent.futures import Future
    from pyprotosdc.consumer.operations import GOperationsManager
    from pyprotosdc.msgreader import MessageReader
    from sdc11073.mdib.statecontainers import (AbstractComplexDeviceComponentStateContainer,
                                               AbstractMetricStateContainer,
                                               AbstractContextStateContainer)
    from decimal import Decimal


class SetServiceWrapper:
    """Consumer-side implementation of SetService"""
    operation_invoked_report = observableproperties.ObservableProperty()
    def __init__(self, channel, msg_reader: MessageReader):
        self._operations_manager = None
        self._mdib_wref = None
        self._logger = logging.getLogger('sdc.grpc.cl.rep_srv')
        self._stub = sdc_services_pb2_grpc.SetServiceStub(channel)
        self._msg_reader = msg_reader

    def set_value(self, operation_handle: str, value: Decimal) -> Future:
        request = sdc_messages_pb2.SetValueRequest()
        request.addressing.action = SdcV1Definitions.Actions.SetValue
        request.addressing.message_id = uuid.uuid4().urn
        request.payload.abstract_set.operation_handle_ref.string = operation_handle
        decimal_to_p(value, request.payload.requested_numeric_value)
        response = self._stub.SetValue(request)
        return self._operations_manager.watch_operation(response)

    def set_string(self, operation_handle: str, value: str) -> Future:
        request = sdc_messages_pb2.SetStringRequest()
        request.addressing.action = SdcV1Definitions.Actions.SetString
        request.addressing.message_id = uuid.uuid4().urn
        request.payload.abstract_set.operation_handle_ref.string = operation_handle
        request.payload.requested_string_value = value
        response = self._stub.SetString(request)
        return self._operations_manager.watch_operation(response)

    def set_metric_state(self, operation_handle: str, proposed_metric_states: list[AbstractMetricStateContainer]) -> Future:
        request = sdc_messages_pb2.SetMetricStateRequest()
        request.addressing.action = SdcV1Definitions.Actions.SetMetricState
        request.addressing.message_id = uuid.uuid4().urn
        request.payload.abstract_set.operation_handle_ref.string = operation_handle
        for proposed in proposed_metric_states:
            p = request.payload.proposed_metric_state.add()
            generic_state_to_p(proposed, p)
        response = self._stub.SetMetricState(request)
        return self._operations_manager.watch_operation(response)

    def activate(self, operation_handle, values) -> Future:
        request = sdc_messages_pb2.ActivateRequest()
        request.addressing.action = SdcV1Definitions.Actions.Activate
        request.addressing.message_id = uuid.uuid4().urn
        request.payload.abstract_set.operation_handle_ref.string = operation_handle
        for v in values:
            p_arg = request.payload.argument.add()
            p_arg.arg_value = str(v).encode('utf-8')
        response = self._stub.Activate(request)
        return self._operations_manager.watch_operation(response)

    def set_component_state(self, operation_handle, pm_component_states:list[AbstractComplexDeviceComponentStateContainer]) -> Future:
        request = sdc_messages_pb2.SetComponentStateRequest()
        request.addressing.action = SdcV1Definitions.Actions.SetComponentState
        request.addressing.message_id = uuid.uuid4().urn
        request.payload.abstract_set.operation_handle_ref.string = operation_handle
        for c in pm_component_states:
            p_component_state = request.payload.proposed_component_state.add()
            generic_state_to_p(c, p_component_state)
        response = self._stub.SetComponentState(request)
        return self._operations_manager.watch_operation(response)

    def set_context_state(self, operation_handle, pm_context_states:list[AbstractContextStateContainer]) -> Future:
        request = sdc_messages_pb2.SetContextStateRequest()
        request.addressing.action = SdcV1Definitions.Actions.SetContextState
        request.addressing.message_id = uuid.uuid4().urn
        request.payload.abstract_set.operation_handle_ref.string = operation_handle
        for c in pm_context_states:
            pm_context_state = request.payload.proposed_context_state.add()
            generic_state_to_p(c, pm_context_state)
        response = self._stub.SetContextState(request)
        return self._operations_manager.watch_operation(response)

    # def register_mdib(self, mdib):
    #     """ Client sometimes must know the mdib data (e.g. Set service, activate method)."""
    #     if mdib is not None and self._mdib_wref is not None:
    #         raise RuntimeError('Client "{}" has already an registered mdib'.format(self.porttype))
    #     self._mdib_wref = None if mdib is None else weakref.ref(mdib)

    def set_operations_manager(self, operations_manager: GOperationsManager):
        self._operations_manager = operations_manager

    def OperationInvokedReport(self):
        self._report_reader = threading.Thread(target=self._read_operation_invoked_reports,
                                               name='read_operation_invoked_reports')
        self._report_reader.daemon = True
        self._report_reader.start()

    def _read_operation_invoked_reports(self):
        """Method runs in background thread"""
        self._logger.info('start reading operation invoked reports')
        request = sdc_messages_pb2.OperationInvokedReportRequest()
        for response in self._stub.OperationInvokedReport(request):
            # write to observable
            self.operation_invoked_report = response
        self._logger.info('stopped reading operation invoked reports')
