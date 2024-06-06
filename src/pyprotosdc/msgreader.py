from __future__ import annotations
from typing import List, TYPE_CHECKING
from .mapping import descriptorsmapper as dm
from .mapping import statesmapper as sm
from .mapping.mapping_helpers import attr_name_to_p, find_one_of_state

class MdibStructureError(Exception):
    pass

if TYPE_CHECKING:
    from org.somda.protosdc.proto.model.biceps.abstractstateoneof_pb2 import AbstractStateOneOfMsg
    from org.somda.protosdc.proto.model import sdc_services_pb2_grpc, sdc_messages_pb2
    from sdc11073.mdib.descriptorcontainers import AbstractDescriptorContainer
    from sdc11073.mdib.statecontainers import AbstractStateContainer
    from pyprotosdc.clientmdib import GClientMdibContainer


class MessageReader(object):
    """ This class does all the conversions from protobuf messages to MDIB objects."""
    def __init__(self, logger, log_prefix=''):
        self._logger = logger
        self._log_prefix = log_prefix

    def read_get_mdib_response(self, response: sdc_messages_pb2.GetMdibResponse) -> tuple[list[AbstractDescriptorContainer], list[AbstractStateContainer]]:
        descriptors = self.read_md_description(response.payload.mdib.md_description)
        descr_by_handle = {d.Handle:d for d in descriptors}
        states = []
        for p_state_one_of in response.payload.mdib.md_state.state:
            p_state = find_one_of_state(p_state_one_of)
            descr_handle = sm.p_get_attr_value(p_state, 'DescriptorHandle').string
            try:
                descr = descr_by_handle[descr_handle]
            except:
                raise
            state = sm.generic_state_from_p(p_state, descr)
            states.append(state)
        return descriptors, states

    @staticmethod
    def _read_abstract_complex_device_component_descriptor_children(p_descr):
        ret = []
        p_acdcd = p_descr.abstract_complex_device_component_descriptor
        attr_name = attr_name_to_p('Handle')
        parent_handle =  getattr(p_acdcd.abstract_device_component_descriptor.abstract_descriptor, attr_name).string
        if p_acdcd.HasField('alert_system'):
            p_alert_system = p_acdcd.alert_system
            alert_system_descriptor = dm.generic_descriptor_from_p(
                p_alert_system, parent_handle)
            ret.append(alert_system_descriptor)
            for p_alert_condition in p_alert_system.alert_condition:
                alert_condition = dm.generic_descriptor_from_p(
                    p_alert_condition, alert_system_descriptor.Handle)
                ret.append(alert_condition)
            for p_alert_signal in p_alert_system.alert_signal:
                alert_condition = dm.generic_descriptor_from_p(
                    p_alert_signal, alert_system_descriptor.Handle)
                ret.append(alert_condition)
        if p_acdcd.HasField('sco'):
            p_sco = p_acdcd.sco
            sco_descriptor = dm.generic_descriptor_from_p(p_sco, parent_handle)
            ret.append(sco_descriptor)
            for p_operation in p_sco.operation:
                alert_condition = dm.generic_descriptor_from_p(
                    p_operation, sco_descriptor.Handle)
                ret.append(alert_condition)
        return ret

    def read_md_description(self, p_md_description_msg) -> List[AbstractDescriptorContainer]:
        ret = []
        for p_mds in p_md_description_msg.mds:
            parent_handle = None
            mds = dm.generic_descriptor_from_p(p_mds, parent_handle)
            ret.append(mds)
            for p_vmd in p_mds.vmd:
                vmd = dm.generic_descriptor_from_p(p_vmd, mds.Handle)
                ret.append(vmd)
                ret.extend(self._read_abstract_complex_device_component_descriptor_children(p_vmd))
                for p_channel in p_vmd.channel:
                    channel = dm.generic_descriptor_from_p(p_channel, vmd.Handle)
                    ret.append(channel)
                    for p_metric in p_channel.metric:
                        metric = dm.generic_descriptor_from_p(p_metric, channel.Handle)
                        ret.append(metric)
            if p_mds.HasField('system_context'):
                p_system_context = p_mds.system_context
                system_context = dm.generic_descriptor_from_p(p_system_context, mds.Handle)
                ret.append(system_context)
                for p_child_list in (p_system_context.ensemble_context,
                                     p_system_context.means_context, p_system_context.operator_context,
                                     p_system_context.workflow_context):
                    for p_child in p_child_list:
                        child_descr = dm.generic_descriptor_from_p(p_child, system_context.Handle)
                        ret.append(child_descr)
                if p_system_context.HasField('location_context'):
                    location_context = dm.generic_descriptor_from_p(
                        p_system_context.location_context, system_context.Handle)
                    ret.append(location_context)
                if p_system_context.HasField('patient_context'):
                    patient_context = dm.generic_descriptor_from_p(
                        p_system_context.patient_context, system_context.Handle)
                    ret.append(patient_context)
            if p_mds.HasField('clock'):
                p_clock = p_mds.clock
                clock_descriptor = dm.generic_descriptor_from_p(p_clock, mds.Handle)
                ret.append(clock_descriptor)
            for p_batt in p_mds.battery:
                battery = dm.generic_descriptor_from_p(p_batt, mds.Handle)
                ret.append(battery)

            ret.extend(self._read_abstract_complex_device_component_descriptor_children(p_mds))
        return ret

    @staticmethod
    def read_states(p_states: list[AbstractStateOneOfMsg], mdib: GClientMdibContainer | None) -> list[AbstractStateContainer]:
        ret = []
        for p_state_one_of in p_states:
            p_state = find_one_of_state(p_state_one_of)
            descr_handle = sm.p_get_attr_value(p_state, 'DescriptorHandle').string
            descr = None if mdib is None else mdib.descriptions.handle.get_one(descr_handle)
            state = sm.generic_state_from_p(p_state, descr)
            ret.append(state)
        return ret

    @staticmethod
    def read_descriptors(p_descriptors, parent_handle):
        ret = []
        for p_descr in p_descriptors:
            descr = dm.generic_descriptor_from_p(p_descr, parent_handle)
            ret.append(descr)
        return ret
