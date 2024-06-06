from __future__ import annotations
from typing import TYPE_CHECKING
import logging
from org.somda.protosdc.proto.model import sdc_services_pb2_grpc
from pyprotosdc.actions import ReportAction

if TYPE_CHECKING:
    from pyprotosdc.provider.subscriptionmgr import GSubscriptionsManager



filter_all_actions = list(ReportAction)

class MdibReportingService(sdc_services_pb2_grpc.MdibReportingServiceServicer):

    def __init__(self, subscriptions_manager: GSubscriptionsManager):
        super().__init__()
        self._subscriptions_manager = subscriptions_manager
        self._subscription = None
        self._logger = logging.getLogger('sdc.grpc.dev.rep_srv')
        self._logger.info('MdibReportingService initialized')

    def EpisodicReport(self, request, context):
        actions = list(request.filter.action_filter.action)
        # empty list means subscribe all
        if not actions:
            actions = filter_all_actions
        self._logger.info('EpisodicReport called')
        subscription = self._subscriptions_manager.on_subscribe_request(actions)
        _run = True
        try:
            while _run:
                report = subscription.reports.get()
                if report == 'stop':
                    _run = False
                else:
                    self._logger.info('yield EpisodicReport %s', report.addressing.action)
                    yield report
        finally:
            self._logger.info('EpisodicReport end')
            self._subscriptions_manager.remove_subscription(subscription)

    def show_fields(self, report):
        try:
            for name in ('alert', 'component', 'context', 'description', 'metric', 'operational_state', 'waveform'):
                self._logger.info('episodic report field %s: %r', name, report.report.HasField(name))
        except:
            pass