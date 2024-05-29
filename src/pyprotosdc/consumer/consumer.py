import logging

import grpc
from org.somda.protosdc.proto.model import sdc_services_pb2_grpc, sdc_messages_pb2
from org.somda.protosdc.proto.model.biceps import setvalue_pb2, abstractset_pb2
from pyprotosdc.consumer.serviceclients.getservice import GetService_Wrapper
from pyprotosdc.consumer.serviceclients.setservice import SetService_Wrapper
from .mdibreportingservice import MdibReportingService_Wrapper, EpisodicReportData
from sdc11073.definitions_sdc import SdcV1Definitions
from sdc11073.xml_types import pm_types
from sdc11073.xml_types import msg_types
from ..msgreader import MessageReader
from sdc11073 import observableproperties as properties
from .operations import GOperationsManager, OperationResult
from ..mapping.basic_mappers import enum_from_p

class GSdcClient:

    # the following observables can be used to observe the incoming notifications by message type.
    # They contain only the body node of the notification, not the envelope
    waveFormReport = properties.ObservableProperty()
    episodicMetricReport = properties.ObservableProperty()
    episodicMetricStates = properties.ObservableProperty()
    episodicAlertReport = properties.ObservableProperty()
    episodicComponentReport = properties.ObservableProperty()
    episodicOperationalStateReport = properties.ObservableProperty()
    episodicContextReport = properties.ObservableProperty()
    descriptionModificationReport = properties.ObservableProperty()
    operationInvokedReport = properties.ObservableProperty()
    anyReport = properties.ObservableProperty()  # all reports can be observed here

    def __init__(self, ip):
        self.channel = grpc.insecure_channel(ip)
        self.sdc_definitions = SdcV1Definitions
        self.log_prefix = ''
        self._logger = logging.getLogger('sdc.client')
        self.msg_reader = MessageReader(self._logger)

        self._clients = {
            "Get": GetService_Wrapper(self.channel, self.msg_reader),
            "Set": SetService_Wrapper(self.channel, self.msg_reader),
            "Event": MdibReportingService_Wrapper(self.channel, self.msg_reader)
        }

        properties.bind(self.client("Event"), episodic_report=self._on_episodic_report)
        properties.bind(self.client("Set"), operation_invoked_report=self._on_operation_invoked_report)
        self.all_subscribed = False

        # start operationInvoked subscription and tell all
        self._operations_manager = GOperationsManager(self.log_prefix)

        for client in self._clients.values():
            if hasattr(client, 'set_operations_manager'):
                client.set_operations_manager(self._operations_manager)


    def client(self, name):
        return self._clients[name]

    def subscribe_all(self):
        service = self.client('Event')
#        self.all_subscribed = True
        service.EpisodicReport()  # starts a background thread to receive stream data
        service = self.client('Set')
        service.OperationInvokedReport()  # starts a background thread to receive stream data
        self.all_subscribed = True

    def _on_episodic_report(self, episodic_report_data: EpisodicReportData):
        print('_on_episodic_report')
        self.anyReport = episodic_report_data
        if episodic_report_data.action == SdcV1Definitions.Actions.Waveform:
            self.waveFormReport = episodic_report_data
        elif episodic_report_data.action == SdcV1Definitions.Actions.EpisodicAlertReport:
            self.episodicAlertReport = episodic_report_data
        elif episodic_report_data.action == SdcV1Definitions.Actions.EpisodicComponentReport:
            self.episodicComponentReport = episodic_report_data
        elif episodic_report_data.action == SdcV1Definitions.Actions.EpisodicContextReport:
            self.episodicContextReport = episodic_report_data
        elif episodic_report_data.action == SdcV1Definitions.Actions.DescriptionModificationReport:
            self.descriptionModificationReport = episodic_report_data
        elif episodic_report_data.action == SdcV1Definitions.Actions.EpisodicMetricReport:
            self.episodicMetricReport = episodic_report_data
        elif episodic_report_data.action == SdcV1Definitions.Actions.EpisodicOperationalStateReport:
            self.episodicOperationalStateReport = episodic_report_data
        else:
            raise ValueError(f'_on_episodic_report: dont know how to handle {episodic_report_data.action}')

    # def _on_operation_invoked_report(self, episodic_report):
    #     self._logger.info('_on_operation_invoked_report with %d report parts', len(episodic_report.operation_invoked.report_part))
    #     for report_part in episodic_report.operation_invoked.report_part:
    #         transaction_id = report_part.invocation_info.transaction_id.unsigned_int
    #         invocation_state = enum_from_p(report_part.invocation_info, 'invocation_state', msg_types.InvocationState)
    #         error = enum_from_p(report_part.invocation_info, 'invocation_error', msg_types.InvocationError)
    #         errormessages = []
    #         try:
    #             print(f'_on_operation_invoked_report tr={transaction_id} state={invocation_state}')
    #             ret = self._operations_manager.on_operation_invoked_report(
    #                 transaction_id, OperationResult(invocation_state, error, errormessages))
    #         except:
    #             import traceback
    #             print(traceback.format_exc())
    #         self.operationInvokedReport = (transaction_id, invocation_state)
    def _on_operation_invoked_report(self, episodic_report):
        self._logger.info('_on_operation_invoked_report with %d report parts', len(episodic_report.operation_invoked.report_part))
        self._operations_manager.on_operation_invoked_report(episodic_report.operation_invoked)
        self.operationInvokedReport = episodic_report.operation_invoked


def run():
    # NOTE(gRPC Python Team): .close() is possible on a channel and should be
    # used in circumstances in which the with statement does not fit the needs
    # of the code.
    with grpc.insecure_channel('localhost:50051') as channel:
        request = sdc_messages_pb2.SetValueRequest(payload=setvalue_pb2.SetValueMsg(
            requested_numeric_value='42',
            abstract_set=abstractset_pb2.AbstractSetMsg(operation_handle_ref='abc')))
        s_stub = sdc_services_pb2_grpc.SetServiceStub(channel)
        response2 = s_stub.SetValue(request)
    print("s_stub client received: " + response2.payload)


if __name__ == '__main__':
    logging.basicConfig()
    run()
