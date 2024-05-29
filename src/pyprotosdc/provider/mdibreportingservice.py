import logging
from org.somda.protosdc.proto.model import sdc_services_pb2_grpc
from org.somda.protosdc.proto.model.sdc_messages_pb2 import EpisodicReportStream
import grpc


class MdibReportingService(sdc_services_pb2_grpc.MdibReportingServiceServicer):

    def __init__(self, subscriptions_manager):
        super().__init__()
        self._subscriptions_manager = subscriptions_manager
        self._subscription = None
        self._logger = logging.getLogger('sdc.grpc.dev.rep_srv')
        self._logger.info('MdibReportingService initialized')

    def EpisodicReport(self, request, context):
        actions = list(request.filter.action_filter.action)
        self._logger.info('EpisodicReport called')
        self._subscription = self._subscriptions_manager.on_subscribe_request(actions)
        _run = True
        while _run:
            report = self._subscription.reports.get()
            if report == 'stop':
                _run = False
            else:
                self._logger.info('yield EpisodicReport %s', report.__class__.__name__)
                self.show_fields(report)
                yield report
                self._logger.info('yield EpisodicReport done')

    def show_fields(self, report):
        try:
            for name in ('alert', 'component', 'context', 'description', 'metric', 'operational_state', 'waveform'):
                self._logger.info('episodic report field %s: %r', name, report.report.HasField(name))
        except:
            pass