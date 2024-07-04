"""Module for conversion of extension elements between xml elements and protosdc ItemMsg.

The value in protosdc is transported as a bytes. This library converts elements to bytes and bytes to elements.
The converter tries to identify the data and calls a specific handler for the identified kind of data.
The sdc classic element is identified by its tag.
The protosdc bytes are identified by a type_url string.

Out of the box the library supports msg_types.Retrievability and sdpi Gender type.
An application can add converters for other types by adding them to the dictionaries
"from_p_handlers" and "from_pm_handlers".
The provided functions must have the signatures:
from_p_func = Callable[[ExtensionMsg.ItemMsg], etree.Element]
from_pm_func = Callable[[etree.Element], ExtensionMsg.ItemMsg]

Elements without an associated converter are transprted as serialized bytes, and type_url is "node_string".

It is also possible to use ExtensionMsg.ItemMsg instances as members in Extension. These types are copied
to the protosdc buffer as they are.
Please note that ExtensionMsg.ItemMsg objects cannot be used with the classic sdc transport!
"""
from __future__ import annotations
from typing import TYPE_CHECKING, Callable
from enum import Enum
from lxml import etree
from org.somda.protosdc.proto.model.biceps.retrievability_pb2 import RetrievabilityMsg
from org.somda.protosdc.proto.model.extension.sdpi.gender_pb2 import GenderMsg

from org.somda.protosdc.proto.model.biceps.extension_pb2 import ExtensionMsg
from sdc11073.xml_types import msg_qnames
from sdc11073.xml_types.pm_types import Retrievability, RetrievabilityInfo, RetrievabilityMethod
from .basic_mappers import enum_attr_to_p, enum_attr_from_p_func, duration_to_p, duration_from_p, enum_from_p
from .mapping_helpers import attr_name_to_p, get_p_attr

if TYPE_CHECKING:
    from sdc11073.xml_types.xml_structure import ExtensionLocalValue

# type_url values that define what the value is and how it can be read
NODE_STRING_TYPE = 'node_string'
RETRIEVABILITY_TYPE = 'org.somda.protosdc.proto.model.biceps.retrievability'
GENDER_TYPE = 'org.somda.protosdc.proto.model.biceps.gender'


def extension_from_pm(pm_src: ExtensionLocalValue, p_dest: ExtensionMsg):
    """Copy content of pm_src to p_dest, and convert data.

    Rules for conversion of the items:
    - item must be an etree Element or an ExtensionMsg.ItemMsg
    - if item is a ExtensionMsg.ItemMsg, append it without any conversion to p_dest
    - else look for handler for tag of element in from_pm_handlers.
    - if there is no handler, treat item as an etree Element.
    - call handler and append its return value to pm_dest.
      The handler has to return an ExtensionMsg.ItemMsg.
    """
    for element in pm_src:
        if isinstance(element, ExtensionMsg.ItemMsg):
            p_dest.item.append(element)
        else:
            handler = (from_pm_handlers.get(element.tag, node_string_from_pm))
            p_dest.item.append(handler(element))


def extension_from_p(pm_dest: ExtensionLocalValue, p_src: ExtensionMsg, pm_name: str):
    """Copy content of p_src to pm_dest, and convert data.

    Rules for conversion of the items:
    - look for handler for type_url in from_p_handlers.
    - if there is a handler for the type_url, call handler and append its return value to pm_dest.
      The handler typically returns an etree Element.
    - if there is no handler, append the item to pm_dest without any conversion.
      It is in the responsibility of the application to convert the data.
    """
    for item in p_src.item:
        handler = from_p_handlers.get(item.extension_data.type_url)
        if handler:
            value = handler(item)
            getattr(pm_dest, pm_name).append(value)
        else:
            # unknown type, no conversion, append the item to ExtensionLocalValue
            getattr(pm_dest, pm_name).append(item)


#  retrievability handling
def retrievability_from_p(item: ExtensionMsg.ItemMsg) -> etree.Element:
    p_retrieve = RetrievabilityMsg.FromString(item.extension_data.value)
    pm_retrieve = Retrievability()
    for p_retrieve_info in p_retrieve.by:
        pm_retrieve_info = RetrievabilityInfo(RetrievabilityMethod.GET)
        value = enum_attr_from_p_func(p_retrieve_info,  attr_name_to_p('Method'), pm_retrieve_info, 'Method')
        pm_retrieve_info.Method = value
        pm_retrieve_info.UpdatePeriod = duration_from_p(get_p_attr(p_retrieve_info, 'UpdatePeriod'))
        pm_retrieve.By.append(pm_retrieve_info)
    elem = pm_retrieve.as_etree_node(msg_qnames.Retrievability, {})
    if item.must_understand:
        elem.attrib['MustUnderstand'] = 'true'
    return elem


def retrievability_from_pm(value: etree.Element) -> ExtensionMsg.ItemMsg:
    pm_retrieve = Retrievability.from_node(value)
    p_retrieve = RetrievabilityMsg()
    for pm_retrieve_info in pm_retrieve.By:
        by = p_retrieve.by.add()
        enum_attr_to_p(pm_retrieve_info.Method, get_p_attr(by, 'Method'))
        duration_to_p(pm_retrieve_info.UpdatePeriod, get_p_attr(by, 'UpdatePeriod'))
    extension_item = ExtensionMsg.ItemMsg()
    extension_item.extension_data.value = p_retrieve.SerializeToString()
    extension_item.extension_data.type_url = RETRIEVABILITY_TYPE
    extension_item.must_understand = value.attrib.get('MustUnderstand') == 'true'
    return extension_item


#  gender handling
class GenderType(str, Enum):
    MALE = 'Male'
    FEMALE = 'Female'
    OTHER = 'Other'
    UNKNOWN = "Unknown"


def gender_from_p(item: ExtensionMsg.ItemMsg) -> etree.Element:
    gender_element = GenderMsg.FromString(item.extension_data.value)
    value = enum_from_p(gender_element, 'gender_type', GenderType)
    ext_node = etree.Element(etree.QName('urn:oid:1.3.6.1.4.1.19376.1.6.2.10.1.1.1', 'Gender'))
    ext_node.text = value
    return ext_node


def gender_from_pm(value: etree.Element) -> ExtensionMsg.ItemMsg:
    p_gender = GenderMsg()
    enum_attr_to_p(value.text, p_gender.gender_type)
    extension_item = ExtensionMsg.ItemMsg()
    extension_item.extension_data.value = p_gender.SerializeToString()
    extension_item.extension_data.type_url = GENDER_TYPE
    extension_item.must_understand = value.attrib.get('MustUnderstand') == 'true'
    return extension_item


# serialized xml handling
def node_string_from_p(item: ExtensionMsg.ItemMsg) -> etree.Element:
    return etree.fromstring(item.extension_data.value)


def node_string_from_pm(value: etree.Element) -> ExtensionMsg.ItemMsg:
    extension_item = ExtensionMsg.ItemMsg()
    extension_item.extension_data.value = etree.tostring(value)
    extension_item.extension_data.type_url = NODE_STRING_TYPE
    extension_item.must_understand = value.attrib.get('MustUnderstand') == 'true'
    return extension_item


from_p_func = Callable[[ExtensionMsg.ItemMsg], etree.Element]


from_pm_func = Callable[[etree.Element], ExtensionMsg.ItemMsg]


from_p_handlers: dict[str, from_p_func] = {
    RETRIEVABILITY_TYPE: retrievability_from_p,
    GENDER_TYPE: gender_from_p,
    NODE_STRING_TYPE: node_string_from_p
}


from_pm_handlers: dict[str, from_pm_func] = {
    msg_qnames.Retrievability: retrievability_from_pm,
    etree.QName('urn:oid:1.3.6.1.4.1.19376.1.6.2.10.1.1.1', 'Gender'): gender_from_pm
}
