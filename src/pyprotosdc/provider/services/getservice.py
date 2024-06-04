from __future__ import annotations
from typing import TYPE_CHECKING
import logging
import traceback
from collections import namedtuple

from org.somda.protosdc.proto.model import sdc_services_pb2_grpc
from org.somda.protosdc.proto.model.sdc_messages_pb2 import (GetMdibResponse,
                                                             GetMdDescriptionResponse,
                                                             GetMdStateResponse,
                                                             GetContextStatesResponse)

from sdc11073.xml_types import pm_qnames
from pyprotosdc.mapping import descriptorsmapper as dm
from pyprotosdc.mapping import statesmapper as sm
from pyprotosdc.mapping.mapping_helpers import (attr_name_to_p,
                                                get_p_attr)

from pyprotosdc.mapping.msgtypes_mappers import set_mdib_version_group

if TYPE_CHECKING:
    from org.somda.protosdc.proto.model.sdc_messages_pb2 import GetMdibRequest


_StackEntry = namedtuple('Stackentry', 'src dest')


def _alert_system_all_to_p(mdib, alert_system_descr, p_parent):
    if alert_system_descr.NODETYPE != pm_qnames.AlertSystemDescriptor:
        raise ValueError('wrong NodeType')
    dm.generic_descriptor_to_p(alert_system_descr, p_parent.abstract_complex_device_component_descriptor.alert_system)
    dest_alert_system = p_parent.abstract_complex_device_component_descriptor.alert_system
    src_as_children = mdib.descriptions.parent_handle.get(alert_system_descr.Handle)
    for src_as_child in src_as_children:
        if src_as_child.NODETYPE == pm_qnames.AlertSignalDescriptor:
            dest_as_child = dest_alert_system.alert_signal.add()
            dm.generic_descriptor_to_p(src_as_child, dest_as_child)
            src_asd_children = mdib.descriptions.parent_handle.get(src_as_child.Handle, [])
            for src_asd_child in src_asd_children:
                raise RuntimeError(f'handling of {src_asd_child.NODETYPE.localname} not implemented')
        elif src_as_child.NODETYPE in (pm_qnames.AlertConditionDescriptor, pm_qnames.LimitAlertConditionDescriptor):
            dest_as_child = dest_alert_system.alert_condition.add()
            dm.generic_descriptor_to_p(src_as_child, dest_as_child)
            src_asd_children = mdib.descriptions.parent_handle.get(src_as_child.Handle, [])
            for src_asd_child in src_asd_children:
                raise RuntimeError(
                    f'handling of {src_asd_child.NODETYPE.localname} not implemented')
        else:
            raise RuntimeError(f'handling of {src_as_child.NODETYPE.localname} not implemented')


def _sco_all_to_p(mdib, sco_descr, p_parent):
    dm.generic_descriptor_to_p(sco_descr,
                               p_parent.abstract_complex_device_component_descriptor.sco)
    src_sco_children = mdib.descriptions.parent_handle.get(sco_descr.Handle, [])
    dest_sco = p_parent.abstract_complex_device_component_descriptor.sco
    for src_sco_child in src_sco_children:
        dest_sco_child = dest_sco.operation.add()
        dm.generic_descriptor_to_p(src_sco_child, dest_sco_child)


def _mdib_to_p(mdib, p_mds_list, p_state_list):
    src_mds_list = mdib.descriptions.NODETYPE.get(pm_qnames.MdsDescriptor)
    for scr_mds in src_mds_list:
        p_mds = p_mds_list.add()  # this creates a new entry in list with correct type
        _mds_to_p(mdib, scr_mds, p_mds)
    _md_state_to_p(mdib.states.objects, p_state_list)


def _md_state_to_p(state_container_list, p_state_list):
    for stateContainer in state_container_list:
        abstract_state_one_of_msg = p_state_list.add()
        sm.generic_state_to_p(stateContainer, abstract_state_one_of_msg)


def _mds_to_p(mdib, scr_mds, p_mds):
    """ reconstruct single mds """
    dm.generic_descriptor_to_p(scr_mds, p_mds)
    # children of mds (vmd, alertsystem, sco, ...)
    mds_children = mdib.descriptions.parent_handle.get(scr_mds.Handle, [])
    for mds_child in mds_children:  # e.g. vmd, sco, alertsystem,...
        if mds_child.NODETYPE == pm_qnames.VmdDescriptor:
            src_vmd = mds_child  # give it a better name for code readability
            dest_vmd = p_mds.vmd.add()
            dm.generic_descriptor_to_p(src_vmd, dest_vmd)
            src_vmd_children = mdib.descriptions.parent_handle.get(src_vmd.Handle, [])
            for src_vmd_child in src_vmd_children:
                if src_vmd_child.NODETYPE == pm_qnames.ChannelDescriptor:
                    dest_vmd_child = dest_vmd.channel.add()
                    dm.generic_descriptor_to_p(src_vmd_child, dest_vmd_child)
                    # dest_vmd.channel.append(dest_vmd_child)
                    src_channel_children = mdib.descriptions.parent_handle.get(src_vmd_child.Handle, [])
                    for src_channel_child in src_channel_children:
                        # children of channels are always metrics
                        dest_metric = dest_vmd_child.metric.add()
                        dm.generic_descriptor_to_p(src_channel_child, dest_metric)
                        # dest_vmd_child.metric.append(dest_metric)
                elif src_vmd_child.NODETYPE == pm_qnames.ScoDescriptor:
                    dest_sco = dest_vmd.abstract_complex_device_component_descriptor.sco
                    _sco_all_to_p(mdib, src_vmd_child, dest_vmd)
                elif src_vmd_child.NODETYPE == pm_qnames.AlertSystemDescriptor:
                    _alert_system_all_to_p(mdib, src_vmd_child, dest_vmd)
                else:
                    raise RuntimeError(f'handling of {src_vmd_child.NODETYPE.localname} not implemented')
        elif mds_child.NODETYPE == pm_qnames.AlertSystemDescriptor:
            _alert_system_all_to_p(mdib, mds_child, p_mds)
        elif mds_child.NODETYPE == pm_qnames.ScoDescriptor:
            _sco_all_to_p(mdib, mds_child, p_mds)
        elif mds_child.NODETYPE == pm_qnames.SystemContextDescriptor:
            src_sc = mds_child  # give it a better name for code readability
            dm.generic_descriptor_to_p(src_sc,
                                       p_mds.system_context)
            src_sc_children = mdib.descriptions.parent_handle.get(src_sc.Handle, [])
            for src_sc_child in src_sc_children:
                p = dm.generic_descriptor_to_p(src_sc_child, None)
                if src_sc_child.NODETYPE == pm_qnames.PatientContextDescriptor:
                    dm.generic_descriptor_to_p(src_sc_child, p_mds.system_context.patient_context)
                elif src_sc_child.NODETYPE == pm_qnames.LocationContextDescriptor:
                    dm.generic_descriptor_to_p(src_sc_child, p_mds.system_context.location_context)
                else:
                    p = dm.generic_descriptor_to_p(src_sc_child, None)
                    if src_sc_child.NODETYPE == pm_qnames.EnsembleContextDescriptor:
                        p_mds.system_context.ensemble_context.append(p)
                    elif src_sc_child.NODETYPE == pm_qnames.MeansContextDescriptor:
                        p_mds.system_context.means_context.append(p)
                    elif src_sc_child.NODETYPE == pm_qnames.OperatorContextDescriptor:
                        p_mds.system_context.operator_context.append(p)
                    elif src_sc_child.NODETYPE == pm_qnames.WorkflowContextDescriptor:
                        p_mds.system_context.workflow_context.append(p)
                    else:
                        raise RuntimeError(
                            f'handling of {src_sc_child.NODETYPE.localname} not implemented')
        elif mds_child.NODETYPE == pm_qnames.ClockDescriptor:
            src_clock = mds_child  # give it a better name for code readability
            dm.generic_descriptor_to_p(src_clock,
                                       p_mds.clock)
            src_clk_children = mdib.descriptions.parent_handle.get(src_clock.Handle, [])
            for src_clk_child in src_clk_children:
                raise RuntimeError(
                    f'handling of {src_clk_child.NODETYPE.localname} not implemented')
        elif mds_child.NODETYPE == pm_qnames.BatteryDescriptor:
            src_batt = mds_child  # give it a better name for code readability
            # battery is a list, src_batt is only one member of it.
            # => add an entry  to p_mds.battery and copy data to it
            dm.generic_descriptor_to_p(src_batt,
                                       p_mds.battery.add())
            src_bat_children = mdib.descriptions.parent_handle.get(src_batt.Handle, [])
            for src_bat_child in src_bat_children:
                raise RuntimeError(
                    f'handling of {src_bat_child.NODETYPE.localname} not implemented')

        else:
            raise RuntimeError(f'handling of {mds_child.NODETYPE.localname} not implemented')


class GetService(sdc_services_pb2_grpc.GetServiceServicer):

    def __init__(self, mdib):
        super().__init__()
        self._mdib = mdib
        self._logger = logging.getLogger('sdc.grpc.dev.GetService')

    def GetMdib(self, request: GetMdibRequest, context) -> GetMdibResponse:
        try:
            response = GetMdibResponse()
            _mdib_to_p(self._mdib, response.payload.mdib.md_description.mds, response.payload.mdib.md_state.state)
            mdib_version_group_msg = getattr(response.payload.mdib, attr_name_to_p('MdibVersionGroup'))
            mdib_version_group_msg1 = get_p_attr(response.payload.abstract_get_response, 'MdibVersionGroup')
            mdib_version_group_msg2 = get_p_attr(response.payload.mdib, 'MdibVersionGroup')
            set_mdib_version_group(mdib_version_group_msg1, self._mdib.mdib_version_group)
            set_mdib_version_group(mdib_version_group_msg2, self._mdib.mdib_version_group)

            getattr(mdib_version_group_msg, attr_name_to_p('MdibVersion')).unsigned_long = self._mdib.mdib_version
            setattr(mdib_version_group_msg, attr_name_to_p('SequenceId'), self._mdib.sequence_id)
            name = attr_name_to_p('InstanceId')
            if mdib_version_group_msg.HasField(name):
                getattr(mdib_version_group_msg, name).value = self._mdib.instance_id
            return response
        except:
            print(traceback.format_exc())
            self._logger.error(traceback.format_exc())
            raise

    def GetMdDescription(self, request, context):
        try:
            response = GetMdDescriptionResponse()
            p_mds_list = response.payload.md_description.mds
            src_mds_list = self._mdib.descriptions.NODETYPE.get(pm_qnames.MdsDescriptor)
            for scr_mds in src_mds_list:
                p_mds = p_mds_list.add()  # this creates a new entry in list with correct type
                _mds_to_p(self._mdib, scr_mds, p_mds)
            return response
        except:
            print(traceback.format_exc())
            self._logger.error(traceback.format_exc())
            raise

    def GetMdState(self, request, context):
        try:
            requested_handles = request.payload.handle_ref
            if not requested_handles:
                states = self._mdib.states.objects
            else:
                states = [self._mdib.states.descriptor_handle.get_one(h.string) for h in requested_handles]
            response = GetMdStateResponse()
            _md_state_to_p(states, response.payload.md_state.state)
            return response
        except:
            print(traceback.format_exc())
            self._logger.error(traceback.format_exc())
            raise

    def GetContextStates(self, request, context):
        try:
            requested_handles = request.payload.handle_ref
            if not requested_handles:
                states = self._mdib.context_states.objects
            else:
                states = [self._mdib.context_states.descriptor_handle.get_one(h.string) for h in requested_handles]
            response = GetContextStatesResponse()
            _md_state_to_p(states, response.payload.context_state)
            return response
        except:
            print(traceback.format_exc())
            self._logger.error(traceback.format_exc())
            raise
