from __future__ import annotations

from typing import TYPE_CHECKING, Any
from concurrent import futures
import traceback
import threading
import time
import uuid
from urllib.parse import quote_plus
import grpc

from org.somda.protosdc.proto.model import sdc_services_pb2_grpc

from pyprotosdc.provider.services.getservice import GetService
from pyprotosdc.provider.services.setservice import SetService
from pyprotosdc.provider.services.mdibreportingservice import MdibReportingService
from . import subscriptionmgr
from pyprotosdc.provider.services.localizationservice import LocalizationService
from ..msgreader import MessageReader
from pyprotosdc.provider.services.archiveservice import ArchiveService
from sdc11073.intervaltimer import IntervalTimer
from sdc11073.exceptions import ApiUsageError
from .sco import ScoOperationsRegistry
from sdc11073.xml_types import pm_types, pm_qnames
from sdc11073.location import SdcLocation
from sdc11073 import loghelper
from sdc11073.roles.protocols import ProductProtocol
from sdc11073.roles.product import DefaultProduct
from sdc11073.roles.waveformprovider.waveformproviderimpl import GenericWaveformProvider
from sdc11073.provider.operations import get_operation_class
from sdc11073 import observableproperties as properties

if TYPE_CHECKING:
    from enum import Enum
    from sdc11073.provider.operations import OperationDefinitionBase
    from sdc11073.mdib.providermdib import ProviderMdib
    from sdc11073.mdib.transactionsprotocol import TransactionResultProtocol
    from sdc11073.xml_types.dpws_types import ThisDeviceType, ThisModelType
    from sdc11073.xml_types import msg_types
    from sdc11073.certloader import SSLContextContainer
    from pyprotosdc.discovery.discoveryimpl import GDiscovery
    from pyprotosdc.mapping.msgtypes_mappers import AnySetServiceRequest


class GSdcProvider(object):
    def __init__(self,
                 g_discovery: GDiscovery,
                 this_model: ThisModelType,
                 this_device: ThisDeviceType,
                 mdib: ProviderMdib,
                 epr: str | None = None,
                 ssl_context_container: SSLContextContainer | None = None,
                 max_subscription_duration: int = 15,
                 socket_timeout: int | float | None = None,
                 log_prefix: str = '' ): #pylint:disable=too-many-arguments
        """Construct a GSdcProvider."""
        if g_discovery is None:
            raise ValueError('g_discovery is None')
        self._g_discovery = g_discovery
        self._log_prefix = log_prefix
        self._sslContext=None
        self._mdib = mdib
        self._subscriptions_manager =  subscriptionmgr.GSubscriptionsManager(self._mdib.sdc_definitions,
                                                                             max_subscription_duration,
                                                                             log_prefix=self._log_prefix)
        self._location = None
        self._server = None
        self._server_thread = None
        self._rtSampleSendThread = None
        self.collectRtSamplesPeriod = 0.1  # in seconds
        self._x_addr: tuple[str, int] | None = None
        self._transaction_id = 0  # central transaction number handling for all called operations.
        self._transaction_id_lock = threading.Lock()

        self.get_service = GetService(self._mdib)
        self.set_service = SetService(self)

        sco_descr_list = self._mdib.descriptions.NODETYPE.get(pm_qnames.ScoDescriptor, [])
        self._sco_operations_registries = {}
        self.product_lookup: dict[str, ProductProtocol] = {}  # one product per sco,  key is a sco handle

        self.waveform_provider = GenericWaveformProvider(self._mdib, self._log_prefix)

        for sco_descr in sco_descr_list:
            sco = self._mkScoOperationsRegistry(sco_descr)
            self._sco_operations_registries[sco_descr.Handle] = sco
            sco.start_worker()
            role_provider = DefaultProduct(self._mdib, sco, self._log_prefix)
            self.product_lookup[sco_descr.Handle] = role_provider

        self.mdib_reporting_service = MdibReportingService(self._subscriptions_manager)
        self.localization_service = LocalizationService(self._mdib)
        self.archive_service = ArchiveService(self._mdib)

        self._port_number = None  # ip listen port
        if epr is None:
            self.epr = uuid.uuid4().urn
        else:
            self.epr = epr

        self._logger = loghelper.get_logger_adapter('sdc.grpc.provider', log_prefix) # logging.getLogger('sdc.device')
        self.msg_reader = MessageReader(self._logger)
        properties.bind(self._mdib, transaction=self._send_episodic_reports)

        # deviceMdibContainer.setSdcDevice(self)


    def start_all(self, startRealtimeSampleLoop=True, shared_http_server=None):
        for pr in self.product_lookup.values():
            pr.init_operations()
        self._server_thread = threading.Thread(target=self._serve, name='grpc_server')
        self._server_thread.daemon = True
        self._server_thread.start()
        time.sleep(1)  # give it some time to start

        if startRealtimeSampleLoop:
            self.start_rt_sample_loop()
            # self._runRtSampleThread = True
            # self._rtSampleSendThread = threading.Thread(target=self._rtSampleSendLoop, name='DevRtSampleSendLoop')
            # self._rtSampleSendThread.daemon = True
            # self._rtSampleSendThread.start()

    def stop_all(self, closeAllConnections=True, sendSubscriptionEnd=True):
        if self._rtSampleSendThread is not None:
            self._runRtSampleThread = False
            self._rtSampleSendThread.join()
            self._rtSampleSendThread = None
        self._subscriptions_manager.stop()
        if self._server:
            self._server.stop(grace=2)

    @property
    def subscriptions_manager(self) ->  subscriptionmgr.GSubscriptionsManager:
        return self._subscriptions_manager


    # def start_realtimesample_loop(self):
    #     if not self._rtSampleSendThread:
    #         self._runRtSampleThread = True
    #         self._rtSampleSendThread = threading.Thread(target=self._rtSampleSendLoop, name='DevRtSampleSendLoop')
    #         self._rtSampleSendThread.daemon = True
    #         self._rtSampleSendThread.start()
    #
    # def stop_realtimesample_loop(self):
    #     if self._rtSampleSendThread:
    #         self._runRtSampleThread = False
    #         self._rtSampleSendThread.join()
    #         self._rtSampleSendThread = None

    def start_rt_sample_loop(self):
        if self.waveform_provider is None:
            raise ApiUsageError('no waveform provider configured.')
        if self.waveform_provider.is_running:
            raise ApiUsageError('realtime send loop already started')
        self.waveform_provider.start()

    def stop_realtime_sample_loop(self):
        if self.waveform_provider is not None and self.waveform_provider.is_running:
            self.waveform_provider.stop()

    @property
    def mdib(self):
        return self._mdib

    def get_operation_by_handle(self, operation_handle: str) -> OperationDefinitionBase | None:
        """Return OperationDefinitionBase for given handle or None if it does not exist."""
        for sco in self._sco_operations_registries.values():
            op = sco.get_operation_by_handle(operation_handle)
            if op is not None:
                return op
        return None

    def handle_operation_request(self,
                                 operation: OperationDefinitionBase,
                                 request: AnySetServiceRequest,
                                 converted_request: msg_types.AbstractSet,
                                 transaction_id: int) -> Enum:
        """Find the responsible sco and forward request to it."""
        for sco in self._sco_operations_registries.values():
            has_this_operation = sco.get_operation_by_handle(operation.handle) is not None
            if has_this_operation:
                return sco.handle_operation_request(operation, request, converted_request, transaction_id)
        self._logger.error('no sco has operation %s', operation.handle)
        return self.mdib.data_model.msg_types.InvocationState.FAILED

    def generate_transaction_id(self) -> int:
        """Return a new transaction id."""
        with self._transaction_id_lock:
            self._transaction_id += 1
            return self._transaction_id

    def _serve(self):
        self._server = grpc.server(futures.ThreadPoolExecutor(max_workers=10,
                                                              thread_name_prefix='grpc_thr_p'))
        sdc_services_pb2_grpc.add_GetServiceServicer_to_server(self.get_service, self._server)
        sdc_services_pb2_grpc.add_SetServiceServicer_to_server(self.set_service, self._server)
        sdc_services_pb2_grpc.add_MdibReportingServiceServicer_to_server(self.mdib_reporting_service, self._server)
        sdc_services_pb2_grpc.add_LocalizationServiceServicer_to_server(self.localization_service, self._server)
        sdc_services_pb2_grpc.add_ArchiveServiceServicer_to_server(self.archive_service, self._server)
        addrs = self._g_discovery.get_active_addresses()
        self._port_number = self._server.add_insecure_port(f'{addrs[0]}:0')
        self._x_addr = (addrs[0], self._port_number)
        self._server.start()
        print('server started')
        self._server.wait_for_termination()
        print('server terminated')

    # def _waveform_updates_transaction(self, changedSamples):
    #     '''
    #     @param changedSamples: a dictionary with key = handle, value= devicemdib.RtSampleArray instance
    #     '''
    #     with self._mdib.mdibUpdateTransaction() as tr:
    #         for descriptorHandle, changedSample in changedSamples.items():
    #             determinationTime = changedSample.determinationTime
    #             samples = [s[0] for s in changedSample.samples]  # only the values without the 'start of cycle' flags
    #             activationState = changedSample.activationState
    #             st = tr.getRealTimeSampleArrayMetricState(descriptorHandle)
    #             if st.metricValue is None:
    #                 st.mkMetricValue()
    #             st.metricValue.Samples = samples
    #             st.metricValue.DeterminationTime = determinationTime  # set Attribute
    #             st.metricValue.Annotations = changedSample.annotations
    #             st.metricValue.ApplyAnnotations = changedSample.applyAnnotations
    #             st.ActivationState = activationState
    #
    # def _rtSampleSendLoop(self):
    #     time.sleep(0.1)  # start delayed in order to have a fully initialized device when waveforms start
    #     timer = IntervalTimer(period_in_seconds=self.collectRtSamplesPeriod)
    #     try:
    #         while self._runRtSampleThread:
    #             behind_schedule_seconds = timer.wait_next_interval_begin()
    #             changed_samples = self._mdib.getUpdatedDeviceRtSamples()
    #             if len(changed_samples) > 0:
    #                 print(f'_rtSampleSendLoop with {len(changed_samples)} waveforms')
    #                 #self._logWaveformTiming(behind_schedule_seconds)
    #                 self._waveform_updates_transaction(changed_samples)
    #             else:
    #                 print('_rtSampleSendLoop no data')
    #         print('_rtSampleSendLoop end')
    #     except Exception as ex:
    #         print(traceback.format_exc())


    def set_location(self,
                     location: SdcLocation,
                     validators: list | None = None,
                     publish_now: bool = True):
        """
        :param location: an SdcLocation instance
        :param validators: a list of pmtypes.InstanceIdentifier objects or None; in that case the defaultInstanceIdentifiers member is used
        :param publish_now: if True, the device is published via its wsdiscovery reference.
        """
        if location == self._location:
            return
        self._location = location
        if validators is None:
            validators = self._mdib.xtra.default_instance_identifiers
        self._mdib.xtra.set_location(location, validators)
        if publish_now:
            self.publish()

    def publish(self):
        """
        publish device on the network (sends HELLO message)
        :return:
        """
        scopes = self._mk_scopes()
        xAddrs = self._get_xaddrs()
        self._g_discovery.publish_service(self.epr, scopes, [f'{self._x_addr[0]}:{self._x_addr[1]}'])

    def _mk_scopes(self) -> list[str]:
        scopes = []
        locations = self._mdib.context_states.NODETYPE.get(pm_qnames.LocationContextState)
        if not locations:
            return scopes
        assoc_loc = [l for l in locations if l.ContextAssociation == pm_types.ContextAssociation.ASSOCIATED]
        for loc in assoc_loc:
            det = loc.LocationDetail
            dr_loc = SdcLocation(fac=det.Facility, poc=det.PoC, bed=det.Bed, bldng=det.Building,
                                 flr=det.Floor, rm=det.Room)
            scopes.append(dr_loc.scope_string)

        for nodetype, scheme in (
                (pm_qnames.OperatorContextDescriptor, 'sdc.ctxt.opr'),
                (pm_qnames.EnsembleContextDescriptor, 'sdc.ctxt.ens'),
                (pm_qnames.WorkflowContextDescriptor, 'sdc.ctxt.wfl'),
                (pm_qnames.MeansContextDescriptor, 'sdc.ctxt.mns'),
        ):
            descriptors = self._mdib.descriptions.NODETYPE.get(nodetype, [])
            for descriptor in descriptors:
                states = self._mdib.context_states.descriptor_handle.get(descriptor.Handle, [])
                assoc_st = [s for s in states if s.ContextAssociation == pm_types.ContextAssociation.ASSOCIATED]
                for st in assoc_st:
                    for ident in st.Identification:
                        scopes.append(f'{scheme}:/{quote_plus(ident.Root)}/{quote_plus(ident.Extension)}')


        scopes.extend(self._get_device_component_based_scopes())
        scopes.append('sdc.mds.pkp:1.2.840.10004.20701.1.1')   # key purpose Service provider
        return scopes

    def _get_device_component_based_scopes(self) -> list[str]:
        """
        SDC: For every instance derived from pm:AbstractComplexDeviceComponentDescriptor in the MDIB an
        SDC SERVICE PROVIDER SHOULD include a URIencoded pm:AbstractComplexDeviceComponentDescriptor/pm:Type
        as dpws:Scope of the MDPWS discovery messages. The URI encoding conforms to the given Extended Backus-Naur Form.
        E.G.  sdc.cdc.type:///69650, sdc.cdc.type:/urn:oid:1.3.6.1.4.1.3592.2.1.1.0//DN_VMD
        After discussion with David: use only MDSDescriptor, VmdDescriptor makes no sense.
        :return: a set of scopes
        """
        scope_strings = set()  # use a set to avoid duplicates
        for t in (pm_qnames.MdsDescriptor,):
            descriptors = self._mdib.descriptions.NODETYPE.get(t)
            for d in descriptors:
                if d.Type is not None:
                    cs = '' if d.Type.CodingSystem == pm_types.DEFAULT_CODING_SYSTEM else d.Type.CodingSystem
                    csv = d.Type.CodingSystemVersion or ''
                    # sc = common_types_pb2.Uri()
                    # sc.value = 'sdc.cdc.type:/{}/{}/{}'.format(cs, csv, d.Type.Code)
                    scope_strings.add('sdc.cdc.type:/{}/{}/{}'.format(cs, csv, d.Type.Code))
        return scope_strings

    def _get_xaddrs(self) -> list[str]:
        srv = self._server
        return [f'{self._x_addr[0]}:{self._x_addr[0]}']
        #xaddrs = self._server

    def _mk_subscription_manager(self, max_subscription_duration):
        return subscriptionmgr.GSubscriptionsManager(self._mdib.sdc_definitions,
                                                     max_subscription_duration,
                                                     log_prefix=self._log_prefix)

    def _mkScoOperationsRegistry(self, sco_descr):
        #Todo: set set_service and op_cls_getter
        op_cls_getter = get_operation_class
        return ScoOperationsRegistry(self._subscriptions_manager,
                                         op_cls_getter,
                                         self._mdib,
                                         sco_descr,
                                         log_prefix=self._log_prefix)

    def _mk_default_role_handlers(self):
        from sdc11073.roles.product import DefaultProduct
        return DefaultProduct(self._mdib,
                              self.scoOperationsRegistry,
                              self._log_prefix)


    def _send_episodic_reports(self, transaction_result: TransactionResultProtocol):
        mdib_version_group = self._mdib.mdib_version_group

        if transaction_result.has_descriptor_updates:
            # port_type_impl = self.hosted_services.description_event_service
            updated = transaction_result.descr_updated
            created = transaction_result.descr_created
            deleted = transaction_result.descr_deleted
            states = transaction_result.all_states()
            self._subscriptions_manager.send_descriptor_updates(
                updated, created, deleted, states, mdib_version_group)

        states = transaction_result.metric_updates
        if len(states) > 0:
            self._subscriptions_manager.send_episodic_metric_report(states, mdib_version_group)
            # self._periodic_reports_handler.store_metric_states(mdib_version_group.mdib_version,
            #                                                    transaction_result.metric_updates)

        states = transaction_result.alert_updates
        if len(states) > 0:
            # port_type_impl = self.hosted_services.state_event_service
            self._subscriptions_manager.send_episodic_alert_report(
                states, mdib_version_group)
            # self._periodic_reports_handler.store_alert_states(mdib_version_group.mdib_version, states)

        states = transaction_result.comp_updates
        if len(states) > 0:
            # port_type_impl = self.hosted_services.state_event_service
            self._subscriptions_manager.send_episodic_component_state_report(
                states, mdib_version_group)
            # self._periodic_reports_handler.store_component_states(mdib_version_group.mdib_version, states)

        states = transaction_result.ctxt_updates
        if len(states) > 0:

            self._subscriptions_manager.send_episodic_context_report(states, mdib_version_group)
            # self._periodic_reports_handler.store_context_states(mdib_version_group.mdib_version, states)

        states = transaction_result.op_updates
        if len(states) > 0:
            # port_type_impl = self.hosted_services.state_event_service
            self._subscriptions_manager.send_episodic_operational_state_report(states, mdib_version_group)
            # self._periodic_reports_handler.store_operational_states(mdib_version_group.mdib_version, states)

        states = transaction_result.rt_updates
        if len(states) > 0:
            # port_type_impl = self.hosted_services.waveform_service
            self._subscriptions_manager.send_realtime_samples_report(states, mdib_version_group)
