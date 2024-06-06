from __future__ import annotations

import logging
from typing import TYPE_CHECKING
from urllib.parse import urlparse
import grpc
from sdc11073 import observableproperties as properties
from sdc11073.definitions_sdc import SdcV1Definitions

from pyprotosdc.consumer.serviceclients.getservice import GetServiceWrapper
from pyprotosdc.consumer.serviceclients.setservice import SetServiceWrapper
from pyprotosdc.consumer.serviceclients.metadataservice import MetadataServiceWrapper
from pyprotosdc.consumer.serviceclients.mdibreportingservice import MdibReportingServiceWrapper, EpisodicReportData
from .operations import GOperationsManager
from ..msgreader import MessageReader

if TYPE_CHECKING:
    from sdc11073.certloader import SSLContextContainer
    from pyprotosdc.discovery.service import Service
    from org.somda.protosdc.proto.model.sdc_messages_pb2 import OperationInvokedReportStream


class GSdcConsumer:
    # the following observables can be used to observe the incoming notifications by message type.
    # They contain only the body node of the notification, not the envelope
    waveform_report = properties.ObservableProperty()
    episodic_metric_report = properties.ObservableProperty()
    episodic_alert_report = properties.ObservableProperty()
    episodic_component_report = properties.ObservableProperty()
    episodic_operational_state_report = properties.ObservableProperty()
    episodic_context_report = properties.ObservableProperty()
    description_modification_report = properties.ObservableProperty()
    operation_invoked_report = properties.ObservableProperty()

    subscription_end_data: properties.ObservableProperty()
    system_error_report: properties.ObservableProperty()

    any_report = properties.ObservableProperty()  # all reports can be observed here

    def __init__(self, ip: str, ssl_context_container: SSLContextContainer | None = None):
        self._ssl_context_container = ssl_context_container
        if ip.startswith('http'):
            netloc = urlparse(ip).netloc
        else:
            netloc = ip
        # Todo: create a secure channel if ssl_context_container is not None
        if ssl_context_container is None:
            self.channel = grpc.insecure_channel(netloc)
        else:
            self.channel = grpc.secure_channel(netloc, self._ssl_context_container.client_context)
        self.sdc_definitions = SdcV1Definitions
        self.log_prefix = ''
        self._logger = logging.getLogger('sdc.client')
        self.msg_reader = MessageReader(self._logger)
        self.metadata_service = MetadataServiceWrapper(self.channel)
        self.get_service = GetServiceWrapper(self.channel, self.msg_reader)
        self.set_service = SetServiceWrapper(self.channel, self.msg_reader)
        self._event_service = MdibReportingServiceWrapper(self.channel, self.msg_reader)

        # start operationInvoked subscription and tell all
        self._operations_manager = GOperationsManager(self.log_prefix)

        properties.bind(self._event_service, episodic_report=self._on_episodic_report)
        properties.bind(self.set_service, operation_invoked_report=self._on_operation_invoked_report)
        self.all_subscribed = False


        for client in (self.get_service, self.set_service, self._event_service):
            if hasattr(client, 'set_operations_manager'):
                client.set_operations_manager(self._operations_manager)

    def subscribe_all(self):
        metadata = self.metadata_service.get_metadata()
        self._event_service.EpisodicReport()  # starts a background thread to receive stream data
        self.set_service.OperationInvokedReport()  # starts a background thread to receive stream data
        self.all_subscribed = True

    def unsubscribe_all(self) -> bool:
        # Todo: How does this work?
        return True

    def _on_episodic_report(self, episodic_report_data: EpisodicReportData):
        """provide data via the usual observables."""
        self.any_report = episodic_report_data
        if episodic_report_data.action == SdcV1Definitions.Actions.Waveform:
            self.waveform_report = episodic_report_data
        elif episodic_report_data.action == SdcV1Definitions.Actions.EpisodicAlertReport:
            self.episodic_alert_report = episodic_report_data
        elif episodic_report_data.action == SdcV1Definitions.Actions.EpisodicComponentReport:
            self.episodic_component_report = episodic_report_data
        elif episodic_report_data.action == SdcV1Definitions.Actions.EpisodicContextReport:
            self.episodic_context_report = episodic_report_data
        elif episodic_report_data.action == SdcV1Definitions.Actions.DescriptionModificationReport:
            self.description_modification_report = episodic_report_data
        elif episodic_report_data.action == SdcV1Definitions.Actions.EpisodicMetricReport:
            self.episodic_metric_report = episodic_report_data
        elif episodic_report_data.action == SdcV1Definitions.Actions.EpisodicOperationalStateReport:
            self.episodic_operational_state_report = episodic_report_data
        else:
            raise ValueError(f'_on_episodic_report: dont know how to handle {episodic_report_data.action}')

    def _on_operation_invoked_report(self, op_invoked_report_stream: OperationInvokedReportStream):
        """provide data via the usual observables."""
        op_invoked = op_invoked_report_stream.operation_invoked
        self._logger.info('_on_operation_invoked_report with %d report parts',
                          len(op_invoked.report_part))
        self._operations_manager.on_operation_invoked_report(op_invoked)
        self.operation_invoked_report = op_invoked

    @classmethod
    def from_service(cls,
                     service: Service,
                     ssl_context_container: SSLContextContainer | None = None):
        """Construct a GSdcConsumer from a Service.

        :param service: a Service instance
        :param ssl_context_container: a ssl context or None
        :return:
        """
        device_locations = service.x_addrs
        if not device_locations:
            raise RuntimeError(f'discovered Service has no address! {service}')
        device_location = device_locations[0]
        return cls(device_location, ssl_context_container)

