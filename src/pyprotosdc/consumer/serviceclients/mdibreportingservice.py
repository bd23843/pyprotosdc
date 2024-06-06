from __future__ import annotations
import logging
import threading
import traceback
from dataclasses import dataclass
from typing import TYPE_CHECKING
from org.somda.protosdc.proto.model import sdc_services_pb2_grpc, sdc_messages_pb2
from sdc11073 import  observableproperties
from sdc11073.mdib.mdibbase import MdibVersionGroup

from pyprotosdc.mapping.mapping_helpers import get_p_attr
from pyprotosdc.mapping.msgtypes_mappers import get_mdib_version_group
from pyprotosdc.actions import ReportAction
if TYPE_CHECKING:
    from pyprotosdc.msgreader import MessageReader
    # from sdc11073.xml_types.actions import Actions


@dataclass
class EpisodicReportData:
    mdib_version_group: MdibVersionGroup
    action: ReportAction
    p_response: sdc_messages_pb2.EpisodicReportStream
    msg_reader: MessageReader


class MdibReportingServiceWrapper:
    episodic_report = observableproperties.ObservableProperty()
    def __init__(self, channel, msg_reader: MessageReader):
        self._stub = sdc_services_pb2_grpc.MdibReportingServiceStub(channel)
        self._logger = logging.getLogger('sdc.grpc.cl.rep_srv')
        self._report_reader_thread: threading.Thread | None = None
        self._msg_reader = msg_reader

    def EpisodicReport(self):
        self._logger.info('EpisodicReport')
        self._report_reader_thread = threading.Thread(target=self._read_episodic_reports, name='read_episodic_reports')
        self._report_reader_thread.daemon = True
        self._report_reader_thread.start()

    def _read_episodic_reports(self):
        """Method is executed in a thread."""
        request = sdc_messages_pb2.EpisodicReportRequest()
        # no filter means all
        # f = request.filter.action_filter.action
        # actions = [ReportAction.Waveform,
        #            ReportAction.DescriptionModificationReport,
        #            ReportAction.EpisodicMetricReport,
        #            ReportAction.EpisodicAlertReport,
        #            ReportAction.EpisodicContextReport,
        #            ReportAction.EpisodicComponentReport,
        #            ReportAction.EpisodicOperationalStateReport]
        # f.extend(actions)
        for response in self._stub.EpisodicReport(request):
            try:
                self.episodic_report = self._map_report(response)
            except:
                self._logger.error(traceback.format_exc())
        print(f'end of EpisodicReports')

    def _map_report(self, report_stream: sdc_messages_pb2.EpisodicReportStream) -> EpisodicReportData:
        report = report_stream.report
        which = report.WhichOneof(report.DESCRIPTOR.oneofs[0].name)
        actual_report = getattr(report, which)

        if which == 'waveform':
            action = ReportAction.Waveform
            mdib_version_group_msg = get_p_attr(actual_report.abstract_report,
                                                'MdibVersionGroup')
        elif which == 'metric':
            action = ReportAction.EpisodicMetricReport
            mdib_version_group_msg = get_p_attr(actual_report.abstract_metric_report.abstract_report,
                                                'MdibVersionGroup')
        elif which == 'alert':
            action = ReportAction.EpisodicAlertReport
            mdib_version_group_msg = get_p_attr(actual_report.abstract_alert_report.abstract_report,
                                                'MdibVersionGroup')
        elif which == 'component':
            action = ReportAction.EpisodicComponentReport
            mdib_version_group_msg = get_p_attr(actual_report.abstract_component_report.abstract_report,
                                                'MdibVersionGroup')
        elif which == 'context':
            action = ReportAction.EpisodicContextReport
            mdib_version_group_msg = get_p_attr(actual_report.abstract_context_report.abstract_report,
                                                'MdibVersionGroup')
        elif which == 'description':
            action = ReportAction.DescriptionModificationReport
            mdib_version_group_msg = get_p_attr(actual_report.abstract_report,
                                                'MdibVersionGroup')
        elif which == 'operational_state':
            action = ReportAction.EpisodicOperationalStateReport
            mdib_version_group_msg = get_p_attr(actual_report.abstract_operational_state_report.abstract_report,
                                                'MdibVersionGroup')
        else:
            raise ValueError(' do not know how to handle report')
        mdib_version_group = get_mdib_version_group(mdib_version_group_msg)
        return EpisodicReportData(mdib_version_group, action, report_stream, self._msg_reader)
