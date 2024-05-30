import unittest
import logging
import time
from sdc11073.loghelper import basic_logging_setup, LoggerAdapter
from pyprotosdc.discovery.discoveryimpl import GDiscovery, filter_services
from pyprotosdc.discovery.service import Service

from org.somda.protosdc.proto.model.discovery import discovery_messages_pb2

class TestDiscovery(unittest.TestCase):
    def setUp(self) -> None:
        basic_logging_setup()
        # logging.getLogger('sdc.discover').setLevel(logging.DEBUG)
        self.disco_provider = GDiscovery('127.0.0.1', logger=logging.getLogger('sdc.discover.provider'))
        self.disco_consumer = GDiscovery('127.0.0.1', logger=logging.getLogger('sdc.discover.consumer'))
        self.disco_provider.start()
        self.disco_consumer.start()

    def tearDown(self):
        self.disco_provider.stop()
        self.disco_consumer.stop()
        pass

    def test_filter_services(self):
        services = [Service(['scope1', 'scope2'],['127.0.0.1:6000'], 'epr1'),
                    Service(['scope1', 'scope3'],['127.0.0.1:6001'], 'epr2'),
                    Service(['sdc.proto://net_loc/abd', 'sdc.proto:/def?arg=1'],['127.0.0.1:6002'], 'epr3'),]
        search_filter1 = discovery_messages_pb2.SearchFilter()
        search_filter1.scope_matcher.algorithm = search_filter1.scope_matcher.STRING_COMPARE
        search_filter1.scope_matcher.scope.value = 'scope1'
        filtered_services = filter_services(services, [search_filter1])
        self.assertEqual(len(filtered_services), 2)

        search_filter2 = discovery_messages_pb2.SearchFilter()
        search_filter2.scope_matcher.algorithm = search_filter2.scope_matcher.STRING_COMPARE
        search_filter2.scope_matcher.scope.value = 'scope2'
        filtered_services = filter_services(services, [search_filter1, search_filter2])
        self.assertEqual(len(filtered_services), 1)

        search_filter3 = discovery_messages_pb2.SearchFilter()
        search_filter3.scope_matcher.algorithm = search_filter3.scope_matcher.STRING_COMPARE
        search_filter3.scope_matcher.scope.value = 'scope3'
        filtered_services = filter_services(services, [search_filter2, search_filter3])
        self.assertEqual(len(filtered_services), 0)

        search_filter_epr3 = discovery_messages_pb2.SearchFilter()
        search_filter_epr3.endpoint_identifier = 'epr3'
        filtered_services = filter_services(services, [search_filter2, search_filter3, search_filter_epr3])
        self.assertEqual(len(filtered_services), 1)

        search_filter_rfc = discovery_messages_pb2.SearchFilter()
        search_filter_rfc.scope_matcher.algorithm = search_filter_rfc.scope_matcher.RFC_3986
        search_filter_rfc.scope_matcher.scope.value = 'sdc.proto://net_loc/abd'
        filtered_services = filter_services(services, [search_filter_rfc])
        self.assertEqual(len(filtered_services), 1)

        search_filter_rfc = discovery_messages_pb2.SearchFilter()
        search_filter_rfc.scope_matcher.algorithm = search_filter_rfc.scope_matcher.RFC_3986
        search_filter_rfc.scope_matcher.scope.value = 'sdc.PROTO://Net_loc/abd'
        filtered_services = filter_services(services, [search_filter_rfc])
        self.assertEqual(len(filtered_services), 1)

        search_filter_rfc = discovery_messages_pb2.SearchFilter()
        search_filter_rfc.scope_matcher.algorithm = search_filter_rfc.scope_matcher.RFC_3986
        search_filter_rfc.scope_matcher.scope.value = 'sdc.proto://net_loc/ABD'
        filtered_services = filter_services(services, [search_filter_rfc])
        self.assertEqual(len(filtered_services), 0)

        pass


    def test_publish(self):
        self.disco_provider.publish_service('my_epr',['scope1', 'scope2'], ['127.0.0.1:50001'])
        pass

    def test_search_all(self):
        self.disco_provider.publish_service('my_epr',['scope1', 'scope2'], ['127.0.0.1:50001'])
        time.sleep(1)
        self.disco_consumer.clear_remote_services()
        services = self.disco_consumer.search_services()
        print(services)
        self.assertEqual(len(services), 1)

    def test_search_scopes(self):
        self.disco_provider.publish_service('my_epr',['scope1', 'scope2'], ['127.0.0.1:50001'])
        self.disco_consumer.clear_remote_services()
        time.sleep(1)
        search_filter = discovery_messages_pb2.SearchFilter()
        search_filter.scope_matcher.algorithm = 2 # string compare
        search_filter.scope_matcher.scope.value = 'scope1'
        services = self.disco_consumer.search_services([search_filter])
        print(services)
        self.assertEqual(len(services), 1)

        self.disco_consumer.clear_remote_services()
        search_filter = discovery_messages_pb2.SearchFilter()
        search_filter.scope_matcher.algorithm = 2 # string compare
        search_filter.scope_matcher.scope.value = 'scope3'
        services = self.disco_consumer.search_services([search_filter])
        print(services)
        self.assertEqual(len(services), 0)
