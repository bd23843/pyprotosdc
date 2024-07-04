import unittest
from decimal import Decimal
from lxml import etree
from sdc11073.xml_types import pm_types
from sdc11073.xml_types import msg_qnames

from sdc11073.mdib import descriptorcontainers as dc
from sdc11073.loghelper import basic_logging_setup
from pyprotosdc.mapping import descriptorsmapper as dm
from pyprotosdc.mapping import extension_mapping
from org.somda.protosdc.proto.model.biceps.extension_pb2 import ExtensionMsg

class TestDescriptorsMapper(unittest.TestCase):
    def setUp(self) -> None:
        basic_logging_setup()

    def tearDown(self) -> None:
        pass

    def check_convert(self, obj):
        obj_p = dm.generic_descriptor_to_p(obj, None)
        obj2 = dm.generic_descriptor_from_p(obj_p, obj.parent_handle)
        self.assertEqual(obj.__class__, obj2.__class__)
        self.assertIsNone(obj.diff(obj2))

    def test_extensions(self):
        """Verify that extension data is correctly mapped.

        In this test all sorts of extensions are added to an mds descriptor, although in reality
        most of them are attached to other descriptors or states. For the test of the mapping itself
        this does not matter.
        """
        mds_max = dc.MdsDescriptorContainer('my_handle', 'p_handle')
        mds_max.ProductionSpecification = [pm_types.ProductionSpecification(pm_types.CodedValue('abc', 'def'), 'prod_spec')]
        mds_max.Manufacturer = [pm_types.LocalizedText('some_company')]

        # an unknown element
        ext_node = etree.Element(etree.QName('www.dummy.com', 'Whatever'))
        etree.SubElement(ext_node, 'foo', attrib={'some_attr': 'some_value'})
        etree.SubElement(ext_node, 'bar', attrib={'another_attr': 'different_value'})
        p_dest = ExtensionMsg()
        extension_mapping.extension_from_pm([ext_node], p_dest)
        self.assertEqual(p_dest.item[0].extension_data.type_url, extension_mapping.NODE_STRING_TYPE)
        mds_max.Extension.append(ext_node)

        # pm_types.Retrievability
        retrievability = pm_types.Retrievability([pm_types.RetrievabilityInfo(pm_types.RetrievabilityMethod.GET,
                                                                              update_period=1.0),
                                                  pm_types.RetrievabilityInfo(pm_types.RetrievabilityMethod.PERIODIC,
                                                                              update_period=42.0),
                                                  ],
                                                 )
        ext_node = retrievability.as_etree_node(msg_qnames.Retrievability, {})
        ext_node.attrib['MustUnderstand'] = 'true'
        mds_max.Extension.append(ext_node)

        p_dest = ExtensionMsg()
        extension_mapping.extension_from_pm([ext_node], p_dest)
        self.assertEqual(p_dest.item[0].extension_data.type_url, extension_mapping.RETRIEVABILITY_TYPE)

        # gender
        ext_node = etree.Element(etree.QName('urn:oid:1.3.6.1.4.1.19376.1.6.2.10.1.1.1', 'Gender'))
        ext_node.text = 'Unknown'
        p_dest = ExtensionMsg()
        extension_mapping.extension_from_pm([ext_node], p_dest)
        self.assertEqual(p_dest.item[0].extension_data.type_url, extension_mapping.GENDER_TYPE)
        mds_max.Extension.append(ext_node)

        self.check_convert(mds_max)
