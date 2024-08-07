import logging
import traceback
import unittest
import uuid
from math import isclose

from org.somda.protosdc.proto.model import sdc_messages_pb2
from org.somda.protosdc.proto.model.biceps.handleref_pb2 import HandleRefMsg
from sdc11073 import loghelper
from sdc11073.definitions_sdc import SdcV1Definitions
from sdc11073.loghelper import basic_logging_setup
from sdc11073.mdib.mdibbase import MdibBase
from sdc11073.xml_types import pm_types

from pyprotosdc.discovery.discoveryimpl import GDiscovery
from pyprotosdc.mapping.mapping_helpers import attr_name_to_p
from pyprotosdc.msgreader import MessageReader
from tests.test_grpc_client_device import SomeProvider


def diff(a: pm_types.PropertyBasedPMType, b: pm_types.PropertyBasedPMType) -> dict:
    ret = {}
    for name, dummy in a.sorted_container_properties():
        try:
            a_value = getattr(a, name)
            b_value = getattr(b, name)
            if a_value == b_value:
                continue
            elif (isinstance(a_value, float) or isinstance(b_value, float)) and isclose(a_value, b_value):
                continue  # float compare (almost equal)
            else:
                ret[name] = (a_value, b_value)
        except (TypeError, AttributeError) as ex:
            ret[name] = ex
    return ret


class TestSomeDeviceGRPC(unittest.TestCase):
    def setUp(self) -> None:
        basic_logging_setup()
        self.wsd = None
        ip_address = '127.0.0.1'
        self.provider_epr = uuid.uuid4().urn
        self.wsd = GDiscovery(ip_address)
        self.wsd.start()
        self.sdc_device = SomeProvider.from_mdib_file(self.wsd, self.provider_epr, 'mdib_two_mds.xml')
        self.sdc_device._mdib.mdibVersion = 42  # start with some non-default value
        self.sdc_device.start_all()

    def tearDown(self) -> None:
        try:
            self.wsd.stop()
        except:
            print(traceback.format_exc())
        try:
            self.sdc_device.stop_all()
        except:
            print(traceback.format_exc())

    def _missing_descriptors(self, device_mdib, client_mdib):
        """ return the descriptors that are present in device_mdib, but not in client_mdib"""
        dev_handles = device_mdib.descriptions.handle.keys()
        cl_handles = client_mdib.descriptions.handle.keys()
        return [h for h in dev_handles if h not in cl_handles]

    def _missing_states(self, device_mdib, client_mdib):
        """ return the states that are present in device_mdib, but not in client_mdib"""
        dev_handles = device_mdib.states.descriptor_handle.keys()
        cl_handles = client_mdib.states.descriptor_handle.keys()
        return [h for h in dev_handles if h not in cl_handles]

    def test_get_mdib(self):
        get_service = self.sdc_device.get_service
        response = get_service.GetMdib(None, None)
        self.assertIsInstance(response, sdc_messages_pb2.GetMdibResponse)
        mdib_version_group_msg = getattr(response.payload.mdib, attr_name_to_p('MdibVersionGroup'))
        mdib_version = getattr(mdib_version_group_msg, attr_name_to_p('MdibVersion')).unsigned_long
        sequence_id = getattr(mdib_version_group_msg, attr_name_to_p('SequenceId'))

        self.assertEqual(mdib_version, self.sdc_device._mdib.mdib_version)
        self.assertEqual(sequence_id, self.sdc_device._mdib.sequence_id)
        if self.sdc_device._mdib.instance_id is None:
            self.assertFalse(mdib_version_group_msg.HasField(attr_name_to_p('InstanceId')))
        else:
            instance__id = getattr(mdib_version_group_msg, attr_name_to_p('InstanceId')).value
            self.assertEqual(instance__id, self.sdc_device._mdib.instance_id)

    def test_get_mdib_msgreader(self):
        get_service = self.sdc_device.get_service
        request = sdc_messages_pb2.GetMdibRequest()
        response = get_service.GetMdib(request, None)
        self.assertIsInstance(response, sdc_messages_pb2.GetMdibResponse)
        reader = MessageReader(logger=logging.getLogger('unittest'))
        cl_mdib = MdibBase(SdcV1Definitions,
                           loghelper.get_logger_adapter('sdc.client.mdib'))
        descriptors = reader.read_md_description(response.payload.mdib.md_description)
        for d in descriptors:
            cl_mdib.descriptions.add_object_no_lock(d)
        missing_descr_handles = self._missing_descriptors(self.sdc_device._mdib, cl_mdib)
        self.assertEqual(len(missing_descr_handles), 0)
        states = reader.read_states(response.payload.mdib.md_state.state, cl_mdib)
        for st in states:
            cl_mdib.states.add_object_no_lock(st)
        missing_state_handles = self._missing_states(self.sdc_device._mdib, cl_mdib)
        self.assertEqual(len(missing_state_handles), 0)

    def test_get_md_description_msgreader(self):
        get_service = self.sdc_device.get_service
        request = sdc_messages_pb2.GetMdDescriptionRequest()
        response = get_service.GetMdDescription(request, None)
        self.assertIsInstance(response, sdc_messages_pb2.GetMdDescriptionResponse)
        reader = MessageReader(logger=logging.getLogger('unittest'))
        cl_mdib = MdibBase(SdcV1Definitions, loghelper.get_logger_adapter('sdc.client.mdib'))
        descriptors = reader.read_md_description(response.payload.md_description)
        for d in descriptors:
            cl_mdib.descriptions.add_object_no_lock(d)
        missing_descr_handles = self._missing_descriptors(self.sdc_device._mdib, cl_mdib)
        self.assertEqual(len(missing_descr_handles), 0)

        not_equal_descriptors = []
        equal_descriptors = []
        for d in descriptors:
            d_ref = self.sdc_device._mdib.descriptions.handle.get_one(d.Handle)
            d_ref.Extension.clear()

            # diff = d_ref.diff(d)
            delta = diff(d_ref, d)
            if delta:
                not_equal_descriptors.append((d_ref, d, delta))
            else:
                equal_descriptors.append(d_ref)

        self.assertEqual(len(not_equal_descriptors), 0)

    def test_get_md_state_msgreader(self):
        reader = MessageReader(logger=logging.getLogger('unittest'))
        cl_mdib = MdibBase(SdcV1Definitions, loghelper.get_logger_adapter('sdc.client.mdib'))
        get_service = self.sdc_device.get_service
        # first get descriptors, otherwise states can't be instantiated
        response = get_service.GetMdDescription(None, None)
        descriptors = reader.read_md_description(response.payload.md_description)
        for d in descriptors:
            cl_mdib.descriptions.add_object_no_lock(d)
        # get all states
        request = sdc_messages_pb2.GetMdStateRequest()
        response = get_service.GetMdState(request, None)
        self.assertIsInstance(response, sdc_messages_pb2.GetMdStateResponse)
        states = reader.read_states(response.payload.md_state.state, cl_mdib)
        for st in states:
            cl_mdib.states.add_object_no_lock(st)
        missing_state_handles = self._missing_states(self.sdc_device._mdib, cl_mdib)
        self.assertEqual(len(missing_state_handles), 0)

        # use handles parameter: try to read only two states, pick some random handles
        all_handles = list(cl_mdib.descriptions.handle.keys())
        handles = [all_handles[4], all_handles[40]]

        for handle in handles:
            msg = HandleRefMsg()
            msg.string = handle
            request.payload.handle_ref.append(msg)

        response = get_service.GetMdState(request, None)
        states = reader.read_states(response.payload.md_state.state, cl_mdib)
        read_state_handles = [st.DescriptorHandle for st in states]
        self.assertEqual(len(states), 2)
        self.assertTrue(handles[0] in read_state_handles)
        self.assertTrue(handles[1] in read_state_handles)

    # def test_activate_valid_handle(self):
    #     reader = MessageReader(logger=logging.getLogger('unittest'))
    #     cl_mdib = MdibBase(SdcV1Definitions, loghelper.get_logger_adapter('sdc.client.mdib'))
    #     setService = self.sdc_device.set_service
    #     request = sdc_messages_pb2.ActivateRequest()
    #     request.payload.abstract_set.operation_handle_ref.string = 'SVO.37.3569'
    #     arg = ActivateMsg.ArgumentMsg()
    #     arg.arg_value = 'bla'
    #     request.payload.argument.append(arg)
    #     response = setService.Activate(request, None)
    #     self.assertIsInstance(response, sdc_messages_pb2.ActivateResponse)
    #     abstract_set_response = response.payload.abstract_set_response
    #     invocation_info = abstract_set_response.invocation_info
    #     self.assertEqual(invocation_info.transaction_id.unsigned_int, self.sdc_device._transaction_id)
    #     self.assertEqual(invocation_info.invocation_state.enum_type, invocation_info.invocation_state.WAIT)
    #     self.assertFalse(invocation_info.HasField('invocation_error'))
    #     self.assertEqual(len(invocation_info.invocation_error_message), 0)
    #
    # def test_activate_invalid_handle(self):
    #     reader = MessageReader(logger=logging.getLogger('unittest'))
    #     cl_mdib = MdibBase(SdcV1Definitions, loghelper.get_logger_adapter('sdc.client.mdib'))
    #     setService = self.sdc_device.set_service
    #     request = sdc_messages_pb2.ActivateRequest()
    #     request.payload.abstract_set.operation_handle_ref.string = 'bla'
    #     response = setService.Activate(request, None)
    #     self.assertIsInstance(response, sdc_messages_pb2.ActivateResponse)
    #     abstract_set_response = response.payload.abstract_set_response
    #     invocation_info = abstract_set_response.invocation_info
    #     self.assertEqual(invocation_info.transaction_id.unsigned_int, self.sdc_device._transaction_id)
    #     self.assertEqual(invocation_info.invocation_state.enum_type, invocation_info.invocation_state.FAIL)
    #     self.assertEqual(invocation_info.invocation_error.enum_type, invocation_info.invocation_error.INV)
    #     self.assertGreater(len(invocation_info.invocation_error_message), 0)
    #
