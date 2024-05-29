from __future__ import annotations
from typing import TYPE_CHECKING
import threading
import queue
import socket
import time
import random
import traceback
from collections import deque
import logging
import select

from org.somda.protosdc.proto.model.discovery import discovery_messages_pb2, discovery_types_pb2, discovery_services_pb2


if TYPE_CHECKING:
    from sdc11073.wsdiscovery.service import Service


MULTICAST_PORT = 3702
MULTICAST_IPV4_ADDRESS = "239.255.255.250"
MULTICAST_OUT_TTL = 15  # Time To Live for multicast_out

UNICAST_UDP_REPEAT = 2
UNICAST_UDP_MIN_DELAY = 50
UNICAST_UDP_MAX_DELAY = 250
UNICAST_UDP_UPPER_DELAY = 500

MULTICAST_UDP_REPEAT = 4
MULTICAST_UDP_MIN_DELAY = 50
MULTICAST_UDP_MAX_DELAY = 250
MULTICAST_UDP_UPPER_DELAY = 500

APP_MAX_DELAY = 500  # miliseconds
BUFFER_SIZE = 0xffff


def _generateInstanceId():
    return str(random.randint(1, 0xFFFFFFFF))

class Message:
    MULTICAST = 'multicast'
    UNICAST = 'unicast'

    def __init__(self, env, addr, port, msgType, initialDelay=0):
        """msgType shall be Message.MULTICAST or Message.UNICAST"""
        self._env = env
        self._addr = addr
        self._port = port
        self._msgType = msgType

        if msgType == self.UNICAST:
            udpRepeat, udpMinDelay, udpMaxDelay, udpUpperDelay = \
                UNICAST_UDP_REPEAT, \
                UNICAST_UDP_MIN_DELAY, \
                UNICAST_UDP_MAX_DELAY, \
                UNICAST_UDP_UPPER_DELAY
        else:
            udpRepeat, udpMinDelay, udpMaxDelay, udpUpperDelay = \
                MULTICAST_UDP_REPEAT, \
                MULTICAST_UDP_MIN_DELAY, \
                MULTICAST_UDP_MAX_DELAY, \
                MULTICAST_UDP_UPPER_DELAY

        self._udpRepeat = udpRepeat
        self._udpUpperDelay = udpUpperDelay
        self._t = (udpMinDelay + ((udpMaxDelay - udpMinDelay) * random.random())) / 2
        self._nextTime = int(time.time() * 1000) + initialDelay

    def getEnv(self):
        return self._env

    def getAddr(self):
        return self._addr

    def getPort(self):
        return self._port

    def msgType(self):
        return self._msgType

    def isFinished(self):
        return self._udpRepeat <= 0

    def canSend(self):
        ct = int(time.time() * 1000)
        return self._nextTime < ct

    def refresh(self):
        self._t = self._t * 2
        if self._t > self._udpUpperDelay:
            self._t = self._udpUpperDelay
        self._nextTime = int(time.time() * 1000) + self._t
        self._udpRepeat = self._udpRepeat - 1


class Service:
    def __init__(self, types, scopes, xAddrs, epr, instanceId):
        self._types = types
        self._scopes = scopes
        self._xAddrs = xAddrs
        self._epr = epr
        self._instanceId = instanceId
        self._messageNumber = 0
        self._metadataVersion = 1

    def getTypes(self):
        return self._types

    def setTypes(self, types):
        self._types = types

    def getScopes(self):
        return self._scopes

    def setScopes(self, scopes):
        self._scopes = scopes

    def getXAddrs(self):
        ret = []
        ipAddrs = None
        for xAddr in self._xAddrs:
            if '{ip}' in xAddr:
                if ipAddrs is None:
                    ipAddrs = _getNetworkAddrs()
                for ipAddr in ipAddrs:
                    if ipAddr not in _IP_BLACKLIST:
                        ret.append(xAddr.format(ip=ipAddr))
            else:
                ret.append(xAddr)
        return ret

    def setXAddrs(self, xAddrs):
        self._xAddrs = xAddrs

    def getEPR(self):
        return self._epr

    def setEPR(self, epr):
        self._epr = epr

    def getInstanceId(self):
        return self._instanceId

    def setInstanceId(self, instanceId):
        self._instanceId = instanceId

    def getMessageNumber(self):
        return self._messageNumber

    def setMessageNumber(self, messageNumber):
        self._messageNumber = messageNumber

    def getMetadataVersion(self):
        return self._metadataVersion

    def setMetadataVersion(self, metadataVersion):
        self._metadataVersion = metadataVersion

    def incrementMessageNumber(self):
        self._messageNumber = self._messageNumber + 1

    def isLocatedOn(self, *ipaddresses):
        '''
        @param ipaddresses: ip addresses, lists of strings or strings
        '''
        my_addresses = []
        for i in ipaddresses:
            if isinstance(i, str):
                my_addresses.append(i)
            else:
                my_addresses.extend(i)
        for addr in self.getXAddrs():
            parsed = urllib.parse.urlsplit(addr)
            ip_addr = parsed.netloc.split(':')[0]
            if ip_addr in my_addresses:
                return True
        return False

    def __repr__(self):
        return 'Service epr={}, instanceId={} Xaddr={} scopes={} types={}'.format(self._epr, self._instanceId,
                                                                          self._xAddrs,
                                                                          ', '.join([str(x) for x in self._scopes]),
                                                                          ', '.join([str(x) for x in self._types]))
    def __str__(self):
        return 'Service epr={}, instanceId={}\n   Xaddr={}\n   scopes={}\n   types={}'.format(self._epr, self._instanceId,
                                                                          self._xAddrs,
                                                                          ', '.join([str(x) for x in self._scopes]),
                                                                          ', '.join([str(x) for x in self._types]))


class _NetworkingThread(object):
    def __init__(self, observer, logger):
        self._recvThread = None
        self._sendThread = None
        self._quitRecvEvent = threading.Event()
        self._quitSendEvent = threading.Event()
        self._queue = queue.Queue(1000)

        self._knownMessageIds = deque()
        self._iidMap = {}
        self._observer = observer
        self._logger = logger

        self._select_in = []

    @staticmethod
    def _createMulticastOutSocket(addr):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setblocking(0)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, MULTICAST_OUT_TTL)
        if addr is None:
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.INADDR_ANY)
        else:
            _addr = socket.inet_aton(addr)
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, _addr)

        return sock

    @staticmethod
    def _createMulticastInSocket():
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        sock.bind(('', MULTICAST_PORT))

        sock.setblocking(0)

        return sock

    def addUnicastMessage(self, env, addr, port, initialDelay=0):
        msg = Message(env, addr, port, Message.UNICAST, initialDelay)
        self._logger.debug('addUnicastMessage: adding message Id %s. delay=%.2f'.format(env.getMessageId(), initialDelay))
        self._queue.put(msg)

    def addMulticastMessage(self, env, addr, port, initialDelay=0):
        msg = Message(env, addr, port, Message.MULTICAST, initialDelay)
        #self._logger.debug('addMulticastMessage: adding message Id %s. delay=%.2f'.format(env.getMessageId(), initialDelay))
        self._queue.put(msg)

    def _run_send(self):
        ''' run by thread'''
        while not self._quitSendEvent.is_set():  # or self._queue:
            if not self._queue.empty():
                try:
                    sz = self._queue.qsize()
                    if sz > 800:
                        self._logger.error('_queue size =%d', sz)
                    elif sz > 500:
                        self._logger.warn('_queue size =%d', sz)
                    elif sz > 100:
                        self._logger.info('_queue size =%d', sz)
                    for dummy in range(self._queue.qsize()):
                        msg = self._queue.get()
                        if msg.canSend():
                            self._sendMsg(msg)
                            msg.refresh()
                            if not msg.isFinished():
                                self._queue.put(msg)
                        else:
                            self._queue.put(msg)
                except:
                    self._logger.error('_run_send:{}'.format(traceback.format_exc()))
                time.sleep(0.02)
            else:
                time.sleep(0.2)

    def _run_recv(self):
        ''' run by thread'''
        while not self._quitRecvEvent.is_set():  # or self._queue:
            try:
                self._recvMessages()
            except:
                if not self._quitRecvEvent.is_set():  # only log error if it does not happen during stop
                    self._logger.error('_run_recv:%s', traceback.format_exc())

    def start(self):
        self._logger.debug('%s: starting ', self.__class__.__name__)
        self._uniOutSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        self._multiInSocket = self._createMulticastInSocket()
        self._register(self._multiInSocket)

        self._multiOutUniInSockets = {}  # FIXME synchronisation

        self._recvThread = threading.Thread(target=self._run_recv, name='wsd.recvThread')
        self._sendThread = threading.Thread(target=self._run_send, name='wsd.sendThread')
        self._recvThread.daemon = True
        self._sendThread.daemon = True
        self._recvThread.start()
        self._sendThread.start()

    def schedule_stop(self):
        """Schedule stopping the thread.
        Use join() to wait, until thread really has been stopped
        """
        self._logger.debug('%s: schedule_stop ', self.__class__.__name__)
        self._quitRecvEvent.set()
        self._quitSendEvent.set()
        self._recvThread.join(1)
        self._sendThread.join(1)
        for sock in self._select_in:
            sock.close()

    def join(self):
        self._logger.debug('%s: join... ', self.__class__.__name__)
        self._recvThread.join()
        self._sendThread.join()
        self._recvThread = None
        self._sendThread = None
        self._uniOutSocket.close()

        self._unregister(self._multiInSocket)
        self._multiInSocket.close()
        self._logger.debug('%s: ... join done', self.__class__.__name__)

    def _register(self, sock):
        self._select_in.append(sock)

    def _unregister(self, sock):
        self._select_in.remove(sock)

    def _recvMessages(self):
        outputs = []
        while self._select_in:
            readable, dummy_writable, dummy_exceptional = select.select(self._select_in, outputs, self._select_in, 0.1)
            if not readable:
                break
            sock = readable[0]
            try:
                data, addr = sock.recvfrom(BUFFER_SIZE)
            except socket.error as e:
                print('socket read error', e)
                time.sleep(0.01)
                continue
#            if self.isFromMySocket(addr):
#                continue
#            #getCommunicationLogger().logDiscoveryMsgIn(addr[0], data)

            env = discovery_messages_pb2.DiscoveryUdpMessage()
            env.ParseFromString(data)
            #env = parseEnvelope(data, addr[0])
            if env is None:  # fault or failed to parse
                continue
            try:
                mid = env.getMessageId()
            except AttributeError:
                print(traceback.format_exc())
                raise
            if mid in self._knownMessageIds:
                self._logger.debug('message Id %s already known. This is a duplicate receive, ignoring.', mid)
                continue
            else:
                self._knownMessageIds.appendleft(mid)
                try:
                    del self._knownMessageIds[-50]  # limit length of remembered message Ids
                except IndexError:
                    pass
            iid = env.getInstanceId()
            mid = env.getMessageId()
            if iid:
                mnum = env.getMessageNumber()
                key = addr[0] + ":" + str(addr[1]) + ":" + str(iid)
                if mid is not None and len(mid) > 0:
                    key = key + ":" + mid
                if not key in self._iidMap:
                    self._iidMap[key] = iid
                else:
                    tmnum = self._iidMap[key]
                    if mnum > tmnum:
                        self._iidMap[key] = mnum
                    else:
                        continue
            self._observer.envReceived(env, addr)

    def _sendMsg(self, msg):
        # action = msg._env.getAction().split('/')[-1]  # only last part
        # if action in ('ResolveMatches', 'ProbeMatches'):
        #     self._logger.debug('_sendMsg: sending %s %s to %s ProbeResolveMatches=%r, epr=%s, repeat=%d msgNo=%r',
        #                        action,
        #                        msg.msgType(),
        #                        msg.getAddr(),
        #                        msg._env.getProbeResolveMatches(),
        #                        msg._env.getEPR(),
        #                        msg._udpRepeat,
        #                        msg._env._messageNumber
        #                        )
        # elif action == 'Probe':
        #     self._logger.debug('_sendMsg: sending %s %s to %s types=%s scopes=%r',
        #                        action,
        #                        msg.msgType(),
        #                        msg.getAddr(),
        #                        _typesinfo(msg._env.getTypes()),
        #                        msg._env.getScopes(),
        #                        )
        # else:
        #     self._logger.debug('_sendMsg: sending %s %s to %s xaddr=%r, epr=%s, repeat=%d msgNo=%r',
        #                        action,
        #                        msg.msgType(),
        #                        msg.getAddr(),
        #                        msg._env.getXAddrs(),
        #                        msg._env.getEPR(),
        #                        msg._udpRepeat,
        #                        msg._env._messageNumber
        #                        )
        #
        data = msg.getEnv().SerializeToString()

        if msg.msgType() == Message.UNICAST:
            #getCommunicationLogger().logDiscoveryMsgOut(msg.getAddr(), data)
            self._uniOutSocket.sendto(data, (msg.getAddr(), msg.getPort()))
        else:
            #getCommunicationLogger().logBroadCastMsgOut(data)
            for sock in self._multiOutUniInSockets.values():
                try:
                    tmp = sock.getsockname()
                except:
                    pass
                sock.sendto(data, (msg.getAddr(), msg.getPort()))


class GDiscovery(threading.Thread):

    def __init__(self, logger=None):
        '''
        @param logger: use this logger. if None a logger 'sdc.discover' is created.
        '''
        self._networkingThread = None
        #self._addrsMonitorThread = None
        self._serverStarted = False
        self._remoteServices = {}
        self._localServices = {}

        #self._dpActive = False  # True if discovery proxy detected (is not relevant in sdc context)
        #self._dpAddr = None
        #self._dpEPR = None

        self._remoteServiceHelloCallback = None
        self._remoteServiceHelloCallbackTypesFilter = None
        self._remoteServiceHelloCallbackScopesFilter = None
        self._remoteServiceByeCallback = None
        self._remoteServiceResolveMatchCallback = None  # B.D.
        self._onProbeCallback = None

        self._logger = logger or logging.getLogger('sdc.discover')
        random.seed((int)(time.time() * 1000000))

    def searchServices(self, types=None, scopes=None, timeout=5, repeatProbeInterval=3):
        '''search for services given the TYPES and SCOPES in a given timeout
        @param repeatProbeInterval: send another probe message after x seconds'''
        if not self._serverStarted:
            raise Exception("Server not started")

        start = time.monotonic()
        end = start + timeout
        now = time.monotonic()
        while now < end:
            self._sendProbe(types, scopes)
            if now + repeatProbeInterval <= end:
                time.sleep(repeatProbeInterval)
            elif now < end:
                time.sleep(end - now)
            now = time.monotonic()
        return filterServices(self._remoteServices.values(), types, scopes)

    def publishService(self, epr, types, scopes, xAddrs):
        """Publish a service with the given TYPES, SCOPES and XAddrs (service addresses)

        if xAddrs contains item, which includes {ip} pattern, one item per IP addres will be sent
        """
        if not self._serverStarted:
            raise Exception("Server not started")

        instanceId = _generateInstanceId()
        service = Service(types, scopes, xAddrs, epr, instanceId)
        self._logger.info('publishing %r', service)
        self._localServices[epr] = service
        self._sendHello(service)

    def start(self):
        'start the discovery server - should be called before using other functions'
        self._startThreads()
        self._serverStarted = True

    def stop(self):
        'cleans up and stops the discovery server'

        self.clearRemoteServices()
        self.clearLocalServices()

        self._stopThreads()
        self._serverStarted = False

    def _startThreads(self):
        if self._networkingThread is not None:
            return
        self._networkingThread = _NetworkingThread(self, self._logger)
        self._networkingThread.start()

    def _stopThreads(self):
        if self._networkingThread is None:
            return
        self._networkingThread.schedule_stop()
        self._networkingThread.join()
        self._networkingThread = None

    def _sendHello(self, service: Service):
        self._logger.info('sending hello on %s', service)
        service.incrementMessageNumber()
        p_msg = discovery_messages_pb2.DiscoveryMessage()
        # env = SoapEnvelope()
        # env.setAction(ACTION_HELLO)
        # env.setTo(ADDRESS_ALL)
        # env.setInstanceId(str(service.getInstanceId()))
        # env.setMessageNumber(str(service.getMessageNumber()))
        # env.setTypes(service.getTypes())
        # env.setScopes(service.getScopes())
        # env.setXAddrs(service.getXAddrs())
        # env.setEPR(service.getEPR())
        for sc in service.getScopes():
            p_msg.hello.endpoint.scope.append(sc)
        # for ty in service.getTypes():
        #     p_ty = p_msg.hello.endpoint.type.add()
        #     p_ty.localName = ty.localname
        #     p_ty.namespace = ty.namespace
        for x_addr in service.getXAddrs():
            uri = p_msg.hello.endpoint.physical_address.add()
            uri.value = x_addr
        self._networkingThread.addMulticastMessage(p_msg, MULTICAST_IPV4_ADDRESS, MULTICAST_PORT,
                                                   random.randint(0, APP_MAX_DELAY))
