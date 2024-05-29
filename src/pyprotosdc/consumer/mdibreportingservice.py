from __future__ import annotations
import logging
import threading
from dataclasses import dataclass
from typing import TYPE_CHECKING
from org.somda.protosdc.proto.model import sdc_services_pb2_grpc, sdc_messages_pb2
from sdc11073.definitions_sdc import SdcV1Definitions
from sdc11073 import  observableproperties
from sdc11073.mdib.mdibbase import MdibVersionGroup

from pyprotosdc.mapping.mapping_helpers import get_p_attr
from pyprotosdc.mapping.msgtypes_mappers import get_mdib_version_group

if TYPE_CHECKING:
    from sdc11073.mdib.descriptorcontainers import AbstractDescriptorContainer
    from sdc11073.mdib.statecontainers import AbstractStateContainer
    from pyprotosdc.msgreader import MessageReader
    from sdc11073.xml_types.actions import Actions

@dataclass
class EpisodicReportData:
    mdib_version_group: MdibVersionGroup
    action: Actions
    p_response: sdc_messages_pb2.EpisodicReportStream
    msg_reader: MessageReader


class MdibReportingService_Wrapper:
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
        request = sdc_messages_pb2.EpisodicReportRequest()
        f = request.filter.action_filter.action
        actions = [SdcV1Definitions.Actions.Waveform,
                   SdcV1Definitions.Actions.DescriptionModificationReport,
                   SdcV1Definitions.Actions.EpisodicMetricReport,
                   SdcV1Definitions.Actions.EpisodicAlertReport,
                   SdcV1Definitions.Actions.EpisodicContextReport,
                   SdcV1Definitions.Actions.EpisodicComponentReport,
                   SdcV1Definitions.Actions.EpisodicOperationalStateReport]
        f.extend(actions)
        for response in self._stub.EpisodicReport(request):
            print(f'Response received')
            self.episodic_report = self._map_report(response)
        print(f'end of EpisodicReports')

    def _map_report(self, report_stream: sdc_messages_pb2.EpisodicReportStream):
        report = report_stream.report
        if report.HasField('waveform'):
            actual_report = report.waveform
            action = SdcV1Definitions.Actions.Waveform
            mdib_version_group_msg = get_p_attr(actual_report.abstract_report,
                                                'MdibVersionGroup')
        # elif report_stream.addressing.action == SdcV1Definitions.Actions.EpisodicMetricReport:
        elif report.HasField('metric'):
            actual_report = report.metric
            action = SdcV1Definitions.Actions.EpisodicMetricReport
            mdib_version_group_msg = get_p_attr(actual_report.abstract_metric_report.abstract_report,
                                                'MdibVersionGroup')
        elif report.HasField('alert'):
            actual_report = report.alert
            action = SdcV1Definitions.Actions.EpisodicAlertReport
            mdib_version_group_msg = get_p_attr(actual_report.abstract_alert_report.abstract_report,
                                                'MdibVersionGroup')
        elif report.HasField('component'):
            actual_report = report.component
            action = SdcV1Definitions.Actions.EpisodicComponentReport
            mdib_version_group_msg = get_p_attr(actual_report.abstract_component_report.abstract_report,
                                                'MdibVersionGroup')
        elif report.HasField('context'):
            actual_report = report.context
            action = SdcV1Definitions.Actions.EpisodicContextReport
            mdib_version_group_msg = get_p_attr(actual_report.abstract_context_report.abstract_report,
                                                'MdibVersionGroup')
        elif report.HasField('description'):
            actual_report = report.description
            action = SdcV1Definitions.Actions.DescriptionModificationReport
            mdib_version_group_msg = get_p_attr(actual_report.abstract_report,
                                                'MdibVersionGroup')
            mdib_version_group = get_mdib_version_group(mdib_version_group_msg)
        elif report.HasField('operational_state'):
            actual_report = report.operational_state
            action = SdcV1Definitions.Actions.EpisodicOperationalStateReport
            mdib_version_group_msg = get_p_attr(actual_report.abstract_report,
                                                'MdibVersionGroup')
        else:
            raise ValueError(' do not know how to handle report')
        mdib_version_group = get_mdib_version_group(mdib_version_group_msg)
        return EpisodicReportData(mdib_version_group, action, report_stream, self._msg_reader)
