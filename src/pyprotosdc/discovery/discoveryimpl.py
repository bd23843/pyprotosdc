from __future__ import annotations

import logging
import platform
import threading
import time
import uuid
from enum import Enum
from typing import TYPE_CHECKING, Callable

from org.somda.protosdc.proto.model.discovery import discovery_messages_pb2, discovery_types_pb2
from sdc11073 import network
from sdc11073.exceptions import ApiUsageError

from .filterimpl import filter_services  # , MULTICAST_PORT, MULTICAST_IPV4_ADDRESS
from .networkingthread import NetworkingThreadPosix, NetworkingThreadWindows, MULTICAST_PORT, MULTICAST_IPV4_ADDRESS
from .service import Service

if TYPE_CHECKING:
    from collections.abc import Iterable
    from logging import Logger
    import ipaddress


class Actions(str, Enum):
    Hello = 'org.somda.protosc.discovery.action.Hello'
    Bye = 'org.somda.protosc.discovery.action.Bye'
    SearchRequest = 'org.somda.protosc.discovery.action.SearchRequest'
    SearchResponse = 'org.somda.protosc.discovery.action.SearchResponse'


def _fill_p_endpoint(service: Service, p_endpoint: discovery_types_pb2.Endpoint):
    """fill p_endpoint with data from Service."""
    p_endpoint.endpoint_identifier = service.epr
    for addr in service.x_addrs:
        uri = p_endpoint.physical_address.add()
        uri.value = addr
    for scope in service.scopes:
        uri = p_endpoint.scope.add()
        uri.value = scope
    pass


def _read_p_endpoint(p_endpoint: discovery_types_pb2.Endpoint) -> Service:
    """Create a Service from p_endpoint."""
    scopes = [uri.value for uri in p_endpoint.scope]
    x_addrs = [uri.value for uri in p_endpoint.physical_address]
    service = Service(scopes, x_addrs, p_endpoint.endpoint_identifier)
    return service


class GDiscovery(threading.Thread):

    def __init__(self,
                 ip_address: str | ipaddress.IPv4Address,
                 logger: Logger | None = None,
                 multicast_port: int = MULTICAST_PORT):
        """Create a WsDiscovery instance.

        :param ip_address: network adapter to bind to
        :param logger: use this logger. if None a logger 'sdc.discover' is created.
        :param multicast_port: defaults to MULTICAST_PORT.
               If port is changed, instance will not be able to communicate with implementations
               that use the correct port (which is the default MULTICAST_PORT)!
        """
        super().__init__(name='GDiscovery thread')
        self._adapter = network.get_adapter_containing_ip(ip_address)
        self._networking_thread = None
        self._addrs_monitor_thread = None
        self._server_started = False
        self._remote_services = {}
        self._local_services = {}
        self._remote_service_hello_callback = None
        self._remote_service_hello_callback_types_filter = None
        self._remote_service_hello_callback_scopes_filter = None
        self._remote_service_bye_callback = None
        self._remote_service_resolve_match_callback = None  # B.D.
        self._on_probe_callback = None

        self._logger = logger or logging.getLogger('sdc.discover')
        self.multicast_port = multicast_port

    def start(self):
        """Start the discovery server - should be called before using other functions."""
        if not self._server_started:
            self._start_threads()
            self._server_started = True

    def stop(self):
        """Clean up and stop the discovery server."""
        if self._server_started:
            self.clear_remote_services()
            self.clear_local_services()

            self._stop_threads()
            self._server_started = False

    def search_services(self,
                        # types: Iterable[QName] | None = None,
                        search_filters: list[discovery_messages_pb2.SearchFilter] | None = None,
                        timeout: int | float | None = 5,
                        repeat_probe_interval: int | None = 3) -> Iterable[Service]:
        """Search for services that match given search filters.

        :param search_filters: list of search filters, no filtering if value is None
        :param timeout: total duration of search
        :param repeat_probe_interval: send another probe message after x seconds
        :return: list[Service]
        """
        if not self._server_started:
            raise RuntimeError("Server not started")

        msg = discovery_messages_pb2.DiscoveryMessage()
        if search_filters:
            for sf in search_filters:
                msg.search_request.search_filter.append(sf)
        msg.addressing.action = Actions.SearchRequest
        msg.addressing.message_id = uuid.uuid4().hex
        self._networking_thread.add_multicast_message(msg, MULTICAST_IPV4_ADDRESS, self.multicast_port)
        time.sleep(timeout)
        services = list(self._remote_services.values())
        if search_filters:
            return filter_services(services, search_filters)
        else:
            return services

    def publish_service(self,
                        epr: str,
                        scopes,
                        x_addrs: list[str]):
        """Publish a service with the given parameters.

        """
        if not self._server_started:
            raise ApiUsageError("Server not started")

        service = Service(scopes, x_addrs, epr)
        self._logger.info('publishing %r', service)
        self._local_services[epr] = service
        self._send_hello(service)

    def clear_remote_services(self):
        """Clear remotely discovered services."""
        self._remote_services.clear()

    def clear_local_services(self):
        """Send Bye messages for the services and remove them."""
        for service in self._local_services.values():
            self._send_bye(service)
        self._local_services.clear()

    def clear_service(self, epr: str):
        """Clear local service with given epr."""
        service = self._local_services[epr]
        self._send_bye(service)
        del self._local_services[epr]

    def get_active_addresses(self) -> list[str]:
        """Get active addresses."""
        # TODO: do not return list
        return [str(self._adapter.ip)]

    def _start_threads(self):
        if self._networking_thread is not None:
            return
        if platform.system() != 'Windows':
            self._networking_thread = NetworkingThreadPosix(str(self._adapter.ip),
                                                            self,
                                                            self._logger,
                                                            self.multicast_port)
        else:
            self._networking_thread = NetworkingThreadWindows(str(self._adapter.ip),
                                                              self,
                                                              self._logger,
                                                              self.multicast_port)
        self._networking_thread.start()

    def _stop_threads(self):
        if self._networking_thread is None:
            return
        self._networking_thread.schedule_stop()
        self._networking_thread.join()
        self._networking_thread = None

    def _send_hello(self, service: Service):
        self._logger.info('sending hello for epr %s', service.epr)
        service.increment_message_number()
        msg = discovery_messages_pb2.DiscoveryMessage()
        _fill_p_endpoint(service, msg.hello.endpoint)
        msg.addressing.action = Actions.Hello
        msg.addressing.message_id = uuid.uuid4().hex
        self._networking_thread.add_multicast_message(msg, MULTICAST_IPV4_ADDRESS, self.multicast_port)

    def _send_bye(self, service: Service):
        self._logger.debug('sending bye for epr "%s"', service.epr)
        msg = discovery_messages_pb2.DiscoveryMessage()
        _fill_p_endpoint(service, msg.bye.endpoint)
        msg.addressing.action = Actions.Bye
        msg.addressing.message_id = uuid.uuid4().hex
        self._networking_thread.add_multicast_message(msg, MULTICAST_IPV4_ADDRESS, self.multicast_port)

    def handle_received_message(self,
                                received_message: discovery_messages_pb2.DiscoveryMessage,
                                addr_from: tuple[str, int]):
        """Forward received message to specific handler (dispatch by action)."""
        action = received_message.addressing.action
        self._logger.debug('handle_received_message: received %s from %s', action.split('/')[-1], addr_from)
        lookup = {Actions.Hello: self._handle_received_hello,
                  Actions.SearchRequest: self._handle_received_search_request,
                  Actions.SearchResponse: self._handle_received_search_response,
                  Actions.Bye: self._handle_received_bye,
                  }
        try:
            func: Callable[[discovery_messages_pb2.DiscoveryMessage, tuple[str, int]], None] = lookup[action]
        except KeyError:
            self._logger.error('unknown action %s', action)
        else:
            func(received_message, addr_from)

    def _handle_received_hello(self,
                               received_message: discovery_messages_pb2.DiscoveryMessage,
                               addr_from: tuple[str, int]):
        self._logger.debug('received Hello from %s',  addr_from)
        hello = received_message.hello
        service = _read_p_endpoint(hello.endpoint)
        self._add_remote_service(service)
        # if self._remote_service_hello_callback is not None:
        #     if matches_filter(service,
        #                       self._remote_service_hello_callback_types_filter,
        #                       self._remote_service_hello_callback_scopes_filter):
        #         self._remote_service_hello_callback(addr_from, service)

    def _handle_received_search_request(self,
                                        received_message: discovery_messages_pb2.DiscoveryMessage,
                                        addr_from: tuple[str, int]):
        self._logger.debug('received SearchRequest from %s', addr_from)
        request = received_message.search_request
        all_services = self._local_services.values()
        filtered_services = filter_services(all_services, request.search_filter)
        response = discovery_messages_pb2.DiscoveryMessage()
        response.addressing.action = Actions.SearchResponse
        response.addressing.message_id = uuid.uuid4().hex
        response.addressing.relates_id.value = received_message.addressing.message_id

        for s in filtered_services:
            ep = response.search_response.endpoint.add()
            _fill_p_endpoint(s, ep)
        # self._networking_thread.add_multicast_message(response, MULTICAST_IPV4_ADDRESS, self.multicast_port)
        self._networking_thread.add_unicast_message(response, addr_from[0], addr_from[1])

    def _handle_received_search_response(self,
                                         received_message: discovery_messages_pb2.DiscoveryMessage,
                                         addr_from: tuple[str, int]):
        self._logger.debug('received SearchResponse from %s', addr_from)
        search_response = received_message.search_response
        services = [_read_p_endpoint(ep) for ep in search_response.endpoint]
        for service in services:
            self._add_remote_service(service)

    def _handle_received_bye(self,
                             received_message: discovery_messages_pb2.DiscoveryMessage,
                             addr_from: tuple[str, int]):
        epr = received_message.bye.endpoint.endpoint_identifier
        self._logger.debug('received Bye from %s, epr = %s', addr_from, epr)
        self._remove_remote_service(epr)

    def _add_remote_service(self, service: Service):
        if not service.epr:
            self._logger.info('service without epr, ignoring it! %r', service)
            return
        already_known_service = self._remote_services.get(service.epr)
        if not already_known_service:
            self._remote_services[service.epr] = service
            self._logger.info('new remote %r', service)
            return

    def _remove_remote_service(self, epr: str):
        if epr in self._remote_services:
            del self._remote_services[epr]
