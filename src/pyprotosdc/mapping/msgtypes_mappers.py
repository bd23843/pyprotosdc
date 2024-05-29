from sdc11073.mdib.mdibbase import MdibVersionGroup
from org.somda.protosdc.proto.model.biceps.mdibversiongroup_pb2 import MdibVersionGroupMsg
from pyprotosdc.mapping.mapping_helpers import get_p_attr, attr_name_to_p
from pyprotosdc.mapping.pmtypesmapper import version_counter_from_p, version_counter_to_p


def get_mdib_version_group(p_data: MdibVersionGroupMsg) -> MdibVersionGroup:
    mdib_version = version_counter_from_p(get_p_attr(p_data, 'MdibVersion'))
    sequence_id = get_p_attr(p_data, 'SequenceId')
    if hasattr(p_data, attr_name_to_p('InstanceId')):
        instance_id = get_p_attr(p_data, 'InstanceId').value
    return MdibVersionGroup(mdib_version, sequence_id, instance_id)

def set_mdib_version_group(p_data: MdibVersionGroupMsg, mdib_version_group: MdibVersionGroup):
    if mdib_version_group.mdib_version > 0:
        version_counter_to_p(mdib_version_group.mdib_version, get_p_attr(p_data, 'MdibVersion'))
    setattr(p_data, attr_name_to_p('SequenceId'), mdib_version_group.sequence_id)
    if mdib_version_group.instance_id is not None:
        get_p_attr(p_data, 'InstanceId').value = mdib_version_group.instance_id
