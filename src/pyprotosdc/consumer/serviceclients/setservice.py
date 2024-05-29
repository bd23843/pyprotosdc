from __future__ import annotations

from typing import TYPE_CHECKING
import weakref
import threading
import logging
from org.somda.protosdc.proto.model import sdc_services_pb2_grpc, sdc_messages_pb2
from org.somda.protosdc.proto.model.biceps import setvalue_pb2, abstractset_pb2
from org.somda.protosdc.proto.model.biceps import setmetricstate_pb2
from sdc11073 import  observableproperties
from pyprotosdc.mapping.statesmapper import generic_state_to_p
from pyprotosdc.mapping.basic_mappers import decimal_to_p


if TYPE_CHECKING:
    from pyprotosdc.consumer.operations import GOperationsManager
    from pyprotosdc.msgreader import MessageReader
    from sdc11073.mdib.statecontainers import AbstractComplexDeviceComponentStateContainer, AbstractMetricStateContainer
    from decimal import Decimal


class SetService_Wrapper:
    """Consumer-side implementation of SetService"""
    operation_invoked_report = observableproperties.ObservableProperty()
    def __init__(self, channel, msg_reader: MessageReader):
        self._operations_manager = None
        self._mdib_wref = None
        self._logger = logging.getLogger('sdc.grpc.cl.rep_srv')
        self._stub = sdc_services_pb2_grpc.SetServiceStub(channel)
        self._msg_reader = msg_reader

    def set_value(self, operation_handle: str, value: Decimal):
        request = sdc_messages_pb2.SetValueRequest()
        request.payload.abstract_set.operation_handle_ref.string = operation_handle
        decimal_to_p(value, request.payload.requested_numeric_value)
        # request.payload.requested_numeric_value = value
        response = self._stub.SetValue(request)
        return self._operations_manager.watch_operation(response)

    def set_string(self, operation_handle: str, value: str):
        request = sdc_messages_pb2.SetStringRequest()
        request.payload.abstract_set.operation_handle_ref.string = operation_handle
        request.payload.requested_string_value = value
        response = self._stub.SetString(request)
        return self._operations_manager.watch_operation(response)

    def set_metric_state(self, operation_handle: str, proposed_metric_states: list[AbstractMetricStateContainer]):
        request = sdc_messages_pb2.SetMetricStateRequest()
        request.payload.abstract_set.operation_handle_ref.string = operation_handle
        for proposed in proposed_metric_states:
            p = request.payload.proposed_metric_state.add()
            generic_state_to_p(proposed, p)
        response = self._stub.SetMetricState(request)
        return self._operations_manager.watch_operation(response)

    def activate(self, operation_handle, values):
        request = sdc_messages_pb2.ActivateRequest()
        request.payload.abstract_set.operation_handle_ref.string = operation_handle
        for v in values:
            p_arg = request.payload.argument.add()
            p_arg.arg_value = str(v).encode('utf-8')
        #request.payload.requested_string_value = value
        response = self._stub.Activate(request)
        return self._operations_manager.watch_operation(response)

    def set_component_state(self, operation_handle, pm_component_states:list[AbstractComplexDeviceComponentStateContainer]):
        request = sdc_messages_pb2.SetComponentStateRequest()
        request.payload.abstract_set.operation_handle_ref.string = operation_handle
        for c in pm_component_states:
            p_component_state = request.payload.proposed_component_state.add()
            generic_state_to_p(c, p_component_state)
        response = self._stub.SetComponentState(request)
        return self._operations_manager.watch_operation(response)

    def register_mdib(self, mdib):
        ''' Client sometimes must know the mdib data (e.g. Set service, activate method).'''
        if mdib is not None and self._mdib_wref is not None:
            raise RuntimeError('Client "{}" has already an registered mdib'.format(self.porttype))
        self._mdib_wref = None if mdib is None else weakref.ref(mdib)

    def set_operations_manager(self, operations_manager: GOperationsManager):
        self._operations_manager = operations_manager

    # def _callOperation(self, soapEnvelope, request_manipulator=None):
    #     return self._operations_manager.callOperation(self, soapEnvelope, request_manipulator)

    def OperationInvokedReport(self):
        self._logger.info('OperationInvokedReport')
        self._report_reader = threading.Thread(target=self._read_operation_invoked_reports, name='read_operation_invoked_reports')
        self._report_reader.daemon = True
        self._report_reader.start()

    def _read_operation_invoked_reports(self):
        request = sdc_messages_pb2.OperationInvokedReportRequest()
        # f = request.filter.action_filter.action
        # actions = [SDC_v1_Definitions.Actions.Waveform,
        #            SDC_v1_Definitions.Actions.DescriptionModificationReport,
        #            SDC_v1_Definitions.Actions.EpisodicMetricReport,
        #            SDC_v1_Definitions.Actions.EpisodicAlertReport,
        #            SDC_v1_Definitions.Actions.EpisodicContextReport,
        #            SDC_v1_Definitions.Actions.EpisodicComponentReport,
        #            SDC_v1_Definitions.Actions.EpisodicOperationalStateReport,
        #            SDC_v1_Definitions.Actions.OperationInvokedReport]
        # f.extend(actions)
        for response in self._stub.OperationInvokedReport(request):
            print(f'OperationInvoked Response received {response.__class__.__name__}')
            self.operation_invoked_report = response
        print(f'end of EpisodicReports')
