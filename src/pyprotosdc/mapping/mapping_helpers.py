from __future__ import annotations

from typing import TYPE_CHECKING, Any

from org.somda.protosdc.proto.model.biceps.localizedtext_pb2 import LocalizedTextMsg
# from sdc11073.mdib.containerproperties import NodeAttributeProperty, NodeAttributeListProperty
from sdc11073.xml_types.xml_structure import _AttributeBase

if TYPE_CHECKING:
    from google.protobuf.internal.python_message import GeneratedProtocolMessageType
    from google.protobuf.message import Message


def name_to_p(name):
    """construct the protobuf member name from biceps name"""
    if name.endswith('Container'):
        name = name[:-9]
    if name.startswith('_'):
        name = name[1:]
    tmp = []
    tmp.append(name[0])
    for c in name[1:]:
        if c.isupper() and tmp[-1].islower():
            tmp.append('_')
        tmp.append(c)
    return ''.join(tmp).lower()


def attr_name_to_p(name: str) -> str:
    """ biceps attributes have an _attr suffix in protobuf"""
    return f'{name_to_p(name)}_attr'


def get_p_attr(parent: GeneratedProtocolMessageType, biceps_attr_name: str) -> Any:
    """return the member of parent that is a BICEPS attribute with name pm_attr_name."""
    return getattr(parent, attr_name_to_p(biceps_attr_name))


def p_name_from_pm_name(p: GeneratedProtocolMessageType, pm_cls: type, pm_name: str) -> str:
    dest_type = getattr(pm_cls, pm_name)
    # determine member name in p:
    if isinstance(dest_type, _AttributeBase):
        p_name = attr_name_to_p(pm_name)
        return p_name
    if pm_name == 'text' and isinstance(p, LocalizedTextMsg):
        p_name = 'localized_text_content'
    elif pm_name == 'ExtExtension':
        p_name = 'extension_element'
    elif pm_name == 'Extension':
        p_name = 'extension_element'
    else:
        p_name = name_to_p(pm_name)
    return p_name


def is_one_of_msg(p: GeneratedProtocolMessageType) -> bool:
    try:
        return len(p.DESCRIPTOR.oneofs) > 0
    except AttributeError:
        return False
    # return p.__class__.__name__.endswith('OneOfMsg')


def find_one_of_state(p: GeneratedProtocolMessageType) -> GeneratedProtocolMessageType:
    if not p.DESCRIPTOR.oneofs:
        # p has no oneof fields
        return p
    which = p.WhichOneof(p.DESCRIPTOR.oneofs[0].name)
    return getattr(p, which)


def find_populated_one_of(p: Message):
    if not is_one_of_msg(p):
        return p
    fields = p.ListFields()
    if len(fields) != 1:
        raise ValueError(f'p {p.__class__.__name__} has {len(fields)} fields, expect exactly one')
    return fields[0][1]


def find_one_of_p_for_container(container, one_of_p):  # , child_classes_lookup):
    """Returns the correct field for a container class inside a one_of_p message"""
    # classes = [ c for c in inspect.getmro(container.__class__) if not c.__name__.startswith('_')]
    p_member_name = name_to_p(container.__class__.__name__)
    try:
        return getattr(one_of_p, p_member_name)
    except AttributeError as ex:
        raise
