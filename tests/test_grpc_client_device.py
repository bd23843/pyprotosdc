from __future__ import annotations
from typing import TYPE_CHECKING
import unittest
import logging
import uuid
import os
import time
from decimal import Decimal
from pyprotosdc.consumer.consumer import GSdcConsumer
from pyprotosdc.provider.provider import GSdcProvider
from pyprotosdc.discovery import GDiscovery
from pyprotosdc.consumer.serviceclients.getservice import GetMdibResponseData
from sdc11073.mdib.providermdib import ProviderMdib
from sdc11073.xml_types.dpws_types import ThisDeviceType, ThisModelType
from sdc11073.xml_types import pm_qnames
from sdc11073.xml_types import pm_types
from sdc11073.xml_types import msg_types
from sdc11073.mdib import descriptorcontainers, statecontainers
from pyprotosdc.clientmdib import GClientMdibContainer
from sdc11073.roles.waveformprovider import waveforms
from sdc11073.loghelper import basic_logging_setup

from sdc11073.location import SdcLocation
from sdc11073.observableproperties import SingleValueCollector
from org.somda.protosdc.proto.model import sdc_messages_pb2
from org.somda.protosdc.proto.model.discovery import discovery_messages_pb2

if TYPE_CHECKING:
    import uuid
    from sdc11073.certloader import SSLContextContainer


class SomeProvider(GSdcProvider):
    """A device used for unit tests

    """
    def __init__(self,
                 g_discovery: GDiscovery,
                 epr: str | uuid.UUID | None,
                 mdib_xml_string: bytes,
                 ssl_context_container: SSLContextContainer | None = None,
                 max_subscription_duration: int = 15,
                 socket_timeout: int | float | None = None,
                 log_prefix: str = ''):
        this_model = ThisModelType(manufacturer='Draeger CoC Systems',
                              manufacturer_url='www.draeger.com',
                              model_name='SomeDevice',
                              model_number='1.0',
                              model_url='www.draeger.com/whatever/you/want/model',
                              presentation_url='www.draeger.com/whatever/you/want/presentation')
        this_device = ThisDeviceType(friendly_name='Py SomeDevice',
                                firmware_version='0.99',
                                serial_number='12345')
        device_mdib_container = ProviderMdib.from_string(mdib_xml_string, log_prefix=log_prefix)
        device_mdib_container.xtra.mk_state_containers_for_all_descriptors()
        # set Metadata
        device_mdib_container.instance_id = 42
        for mds_descriptor in device_mdib_container.descriptions.NODETYPE.get(pm_qnames.MdsDescriptor):
            # mds_descriptor = device_mdib_container.descriptions.NODETYPE.get_one(pm_qnames.MdsDescriptor)
            if mds_descriptor.MetaData is None:
                mds_descriptor.MetaData = pm_types.MetaData()
            mds_descriptor.MetaData.Manufacturer = [pm_types.LocalizedText(u'Dräger')]
            mds_descriptor.MetaData.ModelName = [pm_types.LocalizedText(this_model.ModelName[0].text)]
            mds_descriptor.MetaData.SerialNumber = ['ABCD-1234']
            mds_descriptor.MetaData.ModelNumber = '0.99'
        super().__init__(g_discovery, this_model, this_device, device_mdib_container, epr,
                         ssl_context_container, max_subscription_duration, socket_timeout, log_prefix)
        # self._handler.mkDefaultRoleHandlers()


    @classmethod
    def from_mdib_file(cls,
                       g_discovery: GDiscovery,
                       epr: str | None,
                       mdib_xml_path: str,
                       ssl_context_container: SSLContextContainer | None = None,
                       max_subscription_duration: int = 15,
                       socket_timeout: int | float | None = None,
                       log_prefix: str = ''):
        """
        An alternative constructor for the class
        """
        if not os.path.isabs(mdib_xml_path):
            here = os.path.dirname(__file__)
            mdib_xml_path = os.path.join(here, mdib_xml_path)

        with open(mdib_xml_path, 'rb') as f:
            mdib_xml_string = f.read()
        return cls(g_discovery, epr, mdib_xml_string, ssl_context_container, max_subscription_duration,
                   socket_timeout, log_prefix)


SET_TIMEOUT = 4
NOTIFICATION_TIMEOUT = 2


class TestClientSomeDeviceGRPC(unittest.TestCase):
    def setUp(self) -> None:
        basic_logging_setup()
        ip_address = '127.0.0.1'
        self.provider_epr = uuid.uuid4().urn
        self.wsd = GDiscovery(ip_address)
        self.wsd.start()
        self.sdc_provider = SomeProvider.from_mdib_file(self.wsd, self.provider_epr, 'mdib_two_mds.xml', log_prefix='<Final> ')
        # self.sdc_provider = SomeProvider.from_mdib_file(self.wsd, self.provider_epr, 'mdib_tns.xml', log_prefix='<Final> ')
        self.sdc_provider.mdib.mdibVersion = 42
        self.sdc_provider.start_all(startRealtimeSampleLoop=False)
        time.sleep(0.5)
        self._loc_validators = [pm_types.InstanceIdentifier('Validator', extension_string='System')]
        loc = SdcLocation(fac='tklx', poc='CU1', bed='bed1')
        self.sdc_provider.set_location(loc, self._loc_validators)
        patient_ctxt_descriptors = self.sdc_provider.mdib.descriptions.NODETYPE.get(pm_qnames.PatientContextDescriptor)
        descr = patient_ctxt_descriptors[0]

        pat = statecontainers.PatientContextStateContainer(descr)
        pat.Handle =uuid.uuid4().hex
        pat.CoreData.Givenname = 'Bernd'
        pat.CoreData.Familyname = 'Deichmann'
        self.sdc_provider.mdib.context_states.add_object(pat)

        self.sdc_provider.publish()
        self.dev_sequence_id = self.sdc_provider.mdib.sequence_id
        print(f'{self._testMethodName}: started sdc device, sequence id = {self.sdc_provider.mdib.sequence_id}')
        time.sleep(0.5)
        port = self.sdc_provider._port_number
        self.sdc_consumer = GSdcConsumer(f'{ip_address}:{port}')

    def tearDown(self) -> None:
        self.sdc_provider.stop_all()
        print(f'{self._testMethodName}: stopped sdc device, sequence id = {self.sdc_provider.mdib.sequence_id}')
        self.wsd.stop()

    @staticmethod
    def provideRealtimeData(sdc_provider):
        waveform_provider = sdc_provider.waveform_provider
        paw = waveforms.SawtoothGenerator(min_value=0, max_value=10, waveform_period=1.1, sample_period=0.01)
        waveform_provider.register_waveform_generator('0x34F05500', paw)  # '0x34F05500 MBUSX_RESP_THERAPY2.00H_Paw'

        flow = waveforms.SinusGenerator(min_value=-8.0, max_value=10.0, waveform_period=1.2, sample_period=0.01)
        waveform_provider.register_waveform_generator('0x34F05501', flow)  # '0x34F05501 MBUSX_RESP_THERAPY2.01H_Flow'

        co2 = waveforms.TriangleGenerator(min_value=0, max_value=20, waveform_period=1.0, sample_period=0.01)
        waveform_provider.register_waveform_generator('0x34F05506', co2)  # '0x34F05506 MBUSX_RESP_THERAPY2.06H_CO2_Signal'

        # make SinusGenerator (0x34F05501) the annotator source
        # annotation = pm_types.Annotation(pm_types.CodedValue('a', 'b'))  # what is CodedValue for startOfInspirationCycle?
        waveform_provider.add_annotation_generator(pm_types.CodedValue('a', 'b'),
                                                   trigger_handle='0x34F05501',
                                                   annotated_handles=('0x34F05500', '0x34F05501', '0x34F05506'))

    def test_discover(self):
        disco_consumer = GDiscovery('127.0.0.1', logger=logging.getLogger('sdc.discover.consumer'))
        disco_consumer.start()

        search_filter = discovery_messages_pb2.SearchFilter()
        search_filter.endpoint_identifier = self.provider_epr

        services = disco_consumer.search_services([search_filter], timeout=2)
        print(services)
        self.assertEqual(len(services), 1)

    def test_basic_connect(self):
        "Verify that get_mdib returns a valid result."
        get_service = self.sdc_consumer.get_service
        response = get_service.get_mdib()
        self.assertIsInstance(response, GetMdibResponseData)
        self.assertIsInstance(response.p_response, sdc_messages_pb2.GetMdibResponse)
        self.assertEqual(response.mdib_version_group.mdib_version, self.sdc_provider.mdib.mdib_version)
        self.assertEqual(response.mdib_version_group.sequence_id, self.sdc_provider.mdib.sequence_id)
        self.assertEqual(response.mdib_version_group.instance_id, self.sdc_provider.mdib.instance_id)
        self.assertEqual(len(response.descriptors), len(self.sdc_provider.mdib.descriptions.objects))

    def test_init_mdib(self):
        # self.provideRealtimeData(self.sdc_provider)
        # self.sdc_consumer.subscribe_all()
        cl_mdib = GClientMdibContainer(self.sdc_consumer)
        cl_mdib.init_mdib()
        self.assertEqual(cl_mdib.mdib_version, self.sdc_provider._mdib.mdib_version)
        self.assertEqual(cl_mdib.sequence_id, self.sdc_provider._mdib.sequence_id)
        self.assertEqual(cl_mdib.instance_id, self.sdc_provider._mdib.instance_id)

        self.assertEqual(len(cl_mdib.descriptions.objects), len(self.sdc_provider._mdib.descriptions.objects))
        self.assertEqual(len(cl_mdib.states.objects), len(self.sdc_provider._mdib.states.objects))
        # initial_mdib_version = cl_mdib.mdib_version
        # self.sdc_provider.start_realtimesample_loop()
        # time.sleep(1)
        # self.assertGreater(cl_mdib.mdib_version, initial_mdib_version)

    def test_metric_transaction(self):
        self.sdc_consumer.subscribe_all()
        cl_mdib = GClientMdibContainer(self.sdc_consumer)
        cl_mdib.init_mdib()
        metrics = cl_mdib.descriptions.NODETYPE.get(pm_qnames.NumericMetricDescriptor)
        metric_handle = metrics[0].Handle
        coll = SingleValueCollector(cl_mdib, 'metrics_by_handle')

        with self.sdc_provider.mdib.metric_state_transaction() as tr:
            metric_state = tr.get_state(metric_handle)
            if not metric_state.MetricValue:
                metric_state.mk_metric_value()
            metric_state.MetricValue.Value = Decimal(42)

        # wait for episodic report
        coll.result(timeout=NOTIFICATION_TIMEOUT)
        updated_metric_state = cl_mdib.states.descriptor_handle.get_one(metric_handle)
        self.assertEqual(updated_metric_state.MetricValue.Value, Decimal(42))

    def test_waveform_transaction(self):
        self.sdc_consumer.subscribe_all()
        cl_mdib = GClientMdibContainer(self.sdc_consumer)
        cl_mdib.init_mdib()
        rt_metrics = cl_mdib.descriptions.NODETYPE.get(pm_qnames.RealTimeSampleArrayMetricDescriptor)
        rt_metric_handle = rt_metrics[0].Handle
        coll = SingleValueCollector(cl_mdib, 'waveform_by_handle')

        with self.sdc_provider.mdib.rt_sample_state_transaction() as tr:
            rt_metric_state = tr.get_state(rt_metric_handle)
            if not rt_metric_state.MetricValue:
                rt_metric_state.mk_metric_value()
            rt_metric_state.MetricValue.Samples = [Decimal('42')]
            pass

        # wait for episodic report
        val = coll.result(timeout=NOTIFICATION_TIMEOUT)
        updated_metric_state = cl_mdib.states.descriptor_handle.get_one(rt_metric_handle)
        self.assertEqual(updated_metric_state.MetricValue.Samples[-1], Decimal('42'))

    def test_alert_transaction(self):
        self.sdc_consumer.subscribe_all()
        cl_mdib = GClientMdibContainer(self.sdc_consumer)
        cl_mdib.init_mdib()
        conds = cl_mdib.descriptions.NODETYPE.get(pm_qnames.AlertConditionDescriptor)
        sigs = cl_mdib.descriptions.NODETYPE.get(pm_qnames.AlertSignalDescriptor)
        syss = cl_mdib.descriptions.NODETYPE.get(pm_qnames.AlertSystemDescriptor)

        alert_cond_handle = conds[0].Handle
        alert_sig_handle = sigs[0].Handle
        alert_sys_handle = syss[0].Handle

        coll = SingleValueCollector(cl_mdib, 'alert_by_handle')
        with self.sdc_provider.mdib.alert_state_transaction() as tr:
            alert_cond_state = tr.get_state(alert_cond_handle)
            alert_cond_state.ActualConditionGenerationDelay = 44
            alert_sig_state = tr.get_state(alert_sig_handle)
            alert_sig_state.Slot = 4
            alert_sys_state = tr.get_state(alert_sys_handle)
            alert_sys_state.SelfCheckCount = 555

        # wait for episodic report
        coll.result(timeout=NOTIFICATION_TIMEOUT)
        updated_alert_cond_state = cl_mdib.states.descriptor_handle.get_one(alert_cond_handle)
        self.assertEqual(updated_alert_cond_state.ActualConditionGenerationDelay, 44)
        updated_alert_sig_state = cl_mdib.states.descriptor_handle.get_one(alert_sig_handle)
        self.assertEqual(updated_alert_sig_state.Slot, 4)
        updated_alert_sys_state = cl_mdib.states.descriptor_handle.get_one(alert_sys_handle)
        self.assertGreaterEqual(updated_alert_sys_state.SelfCheckCount, 555)

    def test_descriptor_transaction(self):
        self.sdc_consumer.subscribe_all()
        cl_mdib = GClientMdibContainer(self.sdc_consumer)
        cl_mdib.init_mdib()
        asyss = cl_mdib.descriptions.NODETYPE.get(pm_qnames.AlertSystemDescriptor)
        descriptor_handle = asyss[0].Handle

        coll = SingleValueCollector(cl_mdib, 'updated_descriptors_by_handle')
        with self.sdc_provider.mdib.descriptor_transaction() as tr:
            alert_sys_descr = tr.get_descriptor(descriptor_handle)
            alert_sys_descr.SelfCheckPeriod = 17

        # wait for episodic report
        coll.result(timeout=NOTIFICATION_TIMEOUT)
        updated_alert_sys_descr = cl_mdib.descriptions.handle.get_one(descriptor_handle)
        self.assertEqual(updated_alert_sys_descr.SelfCheckPeriod, 17)

    def test_component_state_transaction(self):
        self.sdc_consumer.subscribe_all()
        cl_mdib = GClientMdibContainer(self.sdc_consumer)
        cl_mdib.init_mdib()

        vmds = cl_mdib.descriptions.NODETYPE.get(pm_qnames.VmdDescriptor)
        vmd_handle = vmds[0].Handle

        coll = SingleValueCollector(cl_mdib, 'component_by_handle')
        with self.sdc_provider.mdib.component_state_transaction() as tr:
            vmd_state = tr.get_state(vmd_handle)
            vmd_state.OperatingHours = 3

        # wait for episodic report
        coll.result(timeout=NOTIFICATION_TIMEOUT)
        updated_vmd_state = cl_mdib.states.descriptor_handle.get_one(vmd_handle)
        self.assertEqual(updated_vmd_state.OperatingHours, 3)

    def test_operational_state_transaction(self):
        self.sdc_consumer.subscribe_all()
        cl_mdib = GClientMdibContainer(self.sdc_consumer)
        cl_mdib.init_mdib()

        operations = cl_mdib.descriptions.NODETYPE.get(pm_qnames.ActivateOperationDescriptor)
        op_handle = operations[0].Handle


        coll = SingleValueCollector(cl_mdib, 'operation_by_handle')
        with (self.sdc_provider.mdib.operational_state_transaction() as tr):
            op_state = tr.get_state(op_handle)
            new_mode = pm_types.OperatingMode.DISABLED \
                if op_state.OperatingMode == pm_types.OperatingMode.ENABLED \
                else pm_types.OperatingMode.ENABLED
            op_state.OperatingMode = new_mode

        # wait for episodic report
        coll.result(timeout=NOTIFICATION_TIMEOUT)
        updated_op_state = cl_mdib.states.descriptor_handle.get_one(op_handle)
        self.assertEqual(updated_op_state.OperatingMode, new_mode)

    def test_context_state_transaction(self):
        self.sdc_consumer.subscribe_all()
        cl_mdib = GClientMdibContainer(self.sdc_consumer)
        cl_mdib.init_mdib()

        coll = SingleValueCollector(cl_mdib, 'context_by_handle')
        new_location = SdcLocation(fac='tklx', poc='CU2', bed='b42')
        self.sdc_provider.mdib.xtra.set_location(new_location)

        # wait for episodic report
        coll.result(timeout=NOTIFICATION_TIMEOUT)
        updated_loc_contexts = cl_mdib.context_states.descriptor_handle.get('LC.mds0', [])
        assocs = [loc for loc in updated_loc_contexts if loc.ContextAssociation == pm_types.ContextAssociation.ASSOCIATED]
        self.assertEqual(1, len(assocs))
        self.assertEqual(assocs[0].LocationDetail.Facility, new_location.fac)
        self.assertEqual(assocs[0].LocationDetail.PoC, new_location.poc)
        self.assertEqual(assocs[0].LocationDetail.Bed, new_location.bed)

    def test_set_metric_state(self):
        self.sdc_consumer.subscribe_all()

        # first we need to add a setMetricState Operation
        sco_descriptors = self.sdc_provider.mdib.descriptions.NODETYPE.get(pm_qnames.ScoDescriptor)
        cls = descriptorcontainers.SetMetricStateOperationDescriptorContainer
        my_code = pm_types.CodedValue('99999')
        factory = self.sdc_provider.mdib.xtra.descriptor_factory
        operation_descriptor_container = factory._create_descriptor_container(cls,
                                                                              'HANDLE_FOR_MY_TEST',
                                                                              sco_descriptors[0].Handle,
                                                                              my_code,
                                                                              pm_types.SafetyClassification.INF)
        metrics = self.sdc_provider.mdib.descriptions.NODETYPE.get(pm_qnames.NumericMetricDescriptor)
        metric_state_handle = metrics[0].Handle

        operation_descriptor_container.OperationTarget = metric_state_handle
        operation_descriptor_container.Type = pm_types.CodedValue('999998')

        scos = self.sdc_provider.mdib.descriptions.NODETYPE.get(pm_qnames.ScoDescriptor)
        sco_handle = scos[0].Handle
        product = self.sdc_provider.product_lookup[sco_handle]
        sco = self.sdc_provider._sco_operations_registries[sco_handle]

        self.sdc_provider.mdib.descriptions.add_object(operation_descriptor_container)
        op = product.metric_provider.make_operation_instance(operation_descriptor_container,
                                                             sco.operation_cls_getter)


        sco.register_operation(op)
        self.sdc_provider.mdib.xtra.mk_state_containers_for_all_descriptors()

        set_service = self.sdc_consumer.set_service
        consumer_mdib = GClientMdibContainer(self.sdc_consumer)
        consumer_mdib.init_mdib()

        operation_handle = operation_descriptor_container.Handle
        proposed_metric_state = consumer_mdib.xtra.mk_proposed_state(metric_state_handle)

        self.assertIsNone(proposed_metric_state.LifeTimePeriod) # just to be sure that we know the correct intitial value
        before_stateversion = proposed_metric_state.StateVersion
        new_life_time_period = 42.5
        proposed_metric_state.LifeTimePeriod = new_life_time_period
        coll = SingleValueCollector(consumer_mdib, 'metrics_by_handle')
        future = set_service.set_metric_state(operation_handle, proposed_metric_states=[proposed_metric_state])
        result = future.result(timeout=SET_TIMEOUT)
        state = result.InvocationInfo.InvocationState
        self.assertEqual(state, msg_types.InvocationState.FINISHED)
        self.assertIsNone(result.InvocationInfo.InvocationError)
        coll.result(timeout=NOTIFICATION_TIMEOUT)
        updated_state = consumer_mdib.states.descriptor_handle.get_one(metric_state_handle)
        self.assertEqual(updated_state.StateVersion, before_stateversion +1)
        self.assertAlmostEqual(updated_state.LifeTimePeriod, new_life_time_period)

    def test_set_string(self):
        self.sdc_consumer.subscribe_all()
        time.sleep(1)
        cl_mdib = GClientMdibContainer(self.sdc_consumer)
        cl_mdib.init_mdib()

        ops = cl_mdib.descriptions.NODETYPE.get(pm_qnames.SetStringOperationDescriptor)
        op_handle = ops[0].Handle
        op_descr = cl_mdib.descriptions.handle.get_one(op_handle)
        op_target_handle = op_descr.OperationTarget
        set_service = self.sdc_consumer.set_service
        target_state = cl_mdib.states.descriptor_handle.get_one(op_target_handle)
        before_stateversion = target_state.StateVersion
        coll = SingleValueCollector(cl_mdib, 'metrics_by_handle')
        future = set_service.set_string(op_handle, 'MEZ')
        result = future.result(timeout=SET_TIMEOUT)
        state = result.InvocationInfo.InvocationState
        self.assertEqual(state, msg_types.InvocationState.FINISHED)
        self.assertIsNone(result.InvocationInfo.InvocationError)
        coll.result(timeout=NOTIFICATION_TIMEOUT)
        updated_state = cl_mdib.states.descriptor_handle.get_one(op_target_handle)
        self.assertEqual(updated_state.StateVersion, before_stateversion +1)

    def test_set_value(self):
        self.sdc_consumer.subscribe_all()

        op_handle = 'HANDLE_FOR_MY_TEST'

        # first we need to add a setValue Operation
        sco_descriptors = self.sdc_provider.mdib.descriptions.NODETYPE.get(pm_qnames.ScoDescriptor)
        cls = descriptorcontainers.SetValueOperationDescriptorContainer
        my_code = pm_types.CodedValue('99999')
        factory = self.sdc_provider.mdib.xtra.descriptor_factory
        operation_descriptor_container = factory._create_descriptor_container(cls,
                                                                              op_handle,
                                                                              sco_descriptors[0].Handle,
                                                                              my_code,
                                                                              pm_types.SafetyClassification.INF)
        metrics = self.sdc_provider.mdib.descriptions.NODETYPE.get(pm_qnames.NumericMetricDescriptor)
        metric_state_handle = metrics[0].Handle
        operation_descriptor_container.OperationTarget = metric_state_handle

        operation_descriptor_container.Type = pm_types.CodedValue('999998')
        self.sdc_provider.mdib.descriptions.add_object(operation_descriptor_container)
        scos = self.sdc_provider.mdib.descriptions.NODETYPE.get(pm_qnames.ScoDescriptor)
        sco_handle = scos[0].Handle
        product = self.sdc_provider.product_lookup[sco_handle]
        sco = self.sdc_provider._sco_operations_registries[sco_handle]

        op = product.metric_provider.make_operation_instance(operation_descriptor_container,
                                                             sco.operation_cls_getter)

        sco.register_operation(op)
        self.sdc_provider.mdib.xtra.mk_state_containers_for_all_descriptors()

        cl_mdib = GClientMdibContainer(self.sdc_consumer)
        cl_mdib.init_mdib()

        # op_descr = cl_mdib.descriptions.handle.get_one(op_handle)
        set_service = self.sdc_consumer.set_service
        future = set_service.set_value(op_handle, Decimal('42'))
        result = future.result(timeout=SET_TIMEOUT)
        state = result.InvocationInfo.InvocationState
        self.assertEqual(state, msg_types.InvocationState.FINISHED)
        self.assertIsNone(result.InvocationInfo.InvocationError)

    def test_activate(self):
        self.sdc_consumer.subscribe_all()

        cl_mdib = GClientMdibContainer(self.sdc_consumer)
        cl_mdib.init_mdib()

        ops = cl_mdib.descriptions.NODETYPE.get(pm_qnames.ActivateOperationDescriptor)
        op_handle = ops[0].Handle
        set_service = self.sdc_consumer.set_service
        future = set_service.activate(op_handle, ['42'])
        result = future.result(timeout=SET_TIMEOUT)
        state = result.InvocationInfo.InvocationState
        self.assertEqual(state, msg_types.InvocationState.FINISHED)
        self.assertIsNone(result.InvocationInfo.InvocationError)

        op_handle = 'invalid_handle'  # an invalid handle
        future = set_service.activate(op_handle, ['42'])
        result = future.result(timeout=SET_TIMEOUT)
        state = result.InvocationInfo.InvocationState
        self.assertEqual(state, msg_types.InvocationState.FAILED)
        self.assertEqual(result.InvocationInfo.InvocationError, msg_types.InvocationError.INVALID_VALUE)
        self.assertGreater(len(result.InvocationInfo.InvocationErrorMessage), 0)

    def test_setComponentState(self):
        self.sdc_consumer.subscribe_all()
        consumer_mdib = GClientMdibContainer(self.sdc_consumer)
        consumer_mdib.init_mdib()

        ops = consumer_mdib.descriptions.NODETYPE.get(pm_qnames.SetComponentStateOperationDescriptor)
        op_handle = ops[0].Handle

        op_descr = consumer_mdib.descriptions.handle.get_one(op_handle)
        op_target_handle = op_descr.OperationTarget
        set_service = self.sdc_consumer.set_service

        proposed_component_state = consumer_mdib.xtra.mk_proposed_state(op_target_handle)
        new_operating_hours = 42
        proposed_component_state.OperatingHours = new_operating_hours

        future = set_service.set_component_state(op_handle, [proposed_component_state])
        result = future.result(timeout=SET_TIMEOUT)
        state = result.InvocationInfo.InvocationState
        self.assertEqual(state, msg_types.InvocationState.FINISHED)
        self.assertIsNone(result.InvocationInfo.InvocationError)

    def test_setContextState(self):
        self.sdc_consumer.subscribe_all()
        consumer_mdib = GClientMdibContainer(self.sdc_consumer)
        consumer_mdib.init_mdib()

        ops = consumer_mdib.descriptions.NODETYPE.get(pm_qnames.SetContextStateOperationDescriptor)
        op_handle = ops[0].Handle

        op_descr = consumer_mdib.descriptions.handle.get_one(op_handle)
        op_target_handle = op_descr.OperationTarget
        set_service = self.sdc_consumer.set_service

        proposed_context_state = consumer_mdib.xtra.mk_proposed_state(op_target_handle)
        proposed_context_state.Handle = proposed_context_state.DescriptorHandle
        future = set_service.set_context_state(op_handle, [proposed_context_state])
        result = future.result(timeout=SET_TIMEOUT)
        state = result.InvocationInfo.InvocationState
        self.assertEqual(state, msg_types.InvocationState.FINISHED)
        self.assertIsNone(result.InvocationInfo.InvocationError)

    def test_get_context_states(self):
        self.sdc_consumer.subscribe_all()
        ret = self.sdc_consumer.get_service.get_context_states()
        self.assertEqual(len(ret.states), len(self.sdc_provider.mdib.context_states.objects))
        pass