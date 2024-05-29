from __future__ import annotations
from typing import TYPE_CHECKING
import uuid
import time
import copy
import socket
import traceback
from collections import deque, defaultdict
import urllib
import http.client
import queue
from lxml import etree as etree_
#from ..namespaces import xmlTag, wseTag, wsaTag, msgTag, nsmap, DocNamespaceHelper
#from ..namespaces import Prefix_Namespace as Prefix
#from .. import isoduration
from sdc11073.etc import short_filter_string
from sdc11073 import observableproperties
from sdc11073.namespaces import default_ns_helper as nsh
from sdc11073 import multikey
from sdc11073 import loghelper
from sdc11073.xml_types.actions import Actions
from org.somda.protosdc.proto.model.sdc_messages_pb2 import EpisodicReportStream
from org.somda.protosdc.proto.model.sdc_messages_pb2 import OperationInvokedReportStream
from ..mapping.statesmapper import generic_state_to_p
from ..mapping.descriptorsmapper import generic_descriptor_to_p
from ..mapping.basic_mappers import enum_attr_to_p
from ..mapping.msgtypes_mappers import set_mdib_version_group
from ..mapping.mapping_helpers import get_p_attr

if TYPE_CHECKING:
    from org.somda.protosdc.proto.model.biceps.descriptionmodificationreport_pb2 import DescriptionModificationReportMsg
    from sdc11073.provider.sco import OperationDefinition
    from sdc11073.mdib.mdibbase import MdibVersionGroup

MAX_ROUNDTRIP_VALUES = 20

class _RoundTripData(object):
    def __init__(self, values, abs_max):
        if values:
            self.values = list(values) # make a copy
            self.min = min(values)
            self.max = max(values)
            self.avg = sum(values)/len(values)
            self.abs_max = abs_max
        else:
            self.values = None
            self.min = None
            self.max = None
            self.avg = None
            self.abs_max = None

    def __repr__(self):
        return 'min={:.4f} max={:.4f} avg={:.4f} absmax={:.4f}'.format(self.min, self.max, self.avg, self.abs_max)


class _GDevSubscription(object):
    MAX_NOTIFY_ERRORS = 1
    IDENT_TAG = etree_.QName('http.local.com', 'MyDevIdentifier')

    def __init__(self, max_subscription_duration, filter_):  # pylint:disable=too-many-arguments
        '''
        @param notifyToAddress: dom node of Subscribe Request
        @param endToAddress: dom node of Subscribe Request
        @param expires: seconds as float
        @param filter: a space separated list of actions, or only one action
        '''
        self.my_identifier = etree_.Element(self.IDENT_TAG)
        self.my_identifier.text = uuid.uuid4().urn

        self._max_subscription_duration = max_subscription_duration
        self._started = None
        self._expireseconds = None
        self.renew(7200)  # sets self._started and self._expireseconds
        self._filters = filter_

        self._notifyErrors = 0
        self._is_closed = False
        self._isConnectionError = False
        self.last_roundtrip_times = deque(
            maxlen=MAX_ROUNDTRIP_VALUES)  # a list of last n roundtrip times for notifications
        self.max_roundtrip_time = 0
        self.reports = queue.Queue(maxsize=50)


    def renew(self, expires):
        self._started = time.monotonic()
        if expires:
            self._expireseconds = min(expires, self._max_subscription_duration)
        else:
            self._expireseconds = self._max_subscription_duration

    # @property
    # def soapClient(self):
    #     return self._soapClient
    #
    @property
    def remainingSeconds(self):
        duration = int(self._expireseconds - (time.monotonic() - self._started))
        return 0 if duration < 0 else duration

    # @property
    # def expireString(self):
    #     return isoduration.durationString(self.remainingSeconds)

    @property
    def hasDeliveryFailure(self):
        return self._notifyErrors >= self.MAX_NOTIFY_ERRORS

    @property
    def hasConnectionError(self):
        return self._isConnectionError

    @property
    def isValid(self):
        if self._is_closed:
            return False
        return self.remainingSeconds > 0 and not self.hasDeliveryFailure

    def matches(self, action):
        action = action.strip()  # just to be sure there are no spaces....
        for f in self._filters:
            if f.endswith(action):
                return True
        return False

    def _mkNotificationReport(self, soapEnvelope, action):
        pass
        # addr = soapenvelope.WsAddress(to=self.notifyToAddress,
        #                               action=action,
        #                               from_=None,
        #                               replyTo=None,
        #                               faultTo=None,
        #                               referenceParametersNode=None)
        # soapEnvelope.setAddress(addr)
        # for identNode in self.notifyRefNodes:
        #     soapEnvelope.addHeaderElement(identNode)
        # soapEnvelope.validateBody(self._bicepsSchema.bmmSchema)
        # return soapEnvelope

    def _mkEndReport(self, soapEnvelope, action):
        pass
        # to_addr = self.endToAddress or self.notifyToAddress
        # addr = soapenvelope.WsAddress(to=to_addr,
        #                               action=action,
        #                               from_=None,
        #                               replyTo=None,
        #                               faultTo=None,
        #                               referenceParametersNode=None)
        # soapEnvelope.setAddress(addr)
        # ref_nodes = self.endToRefNodes or self.notifyRefNodes
        # for identNode in ref_nodes:
        #     identNode_ = copy.copy(identNode)
        #     # mandatory attribute acc. to ws_addressing SOAP Binding (https://www.w3.org/TR/2006/REC-ws-addr-soap-20060509/)
        #     identNode_.set('IsReferenceParameter', 'true')
        #     soapEnvelope.addHeaderElement(identNode_)
        # return soapEnvelope

    def send_notification_report(self, report):
        self.reports.put(report)
        # if not self.isValid:
        #     return
        # soapEnvelope = soapenvelope.Soap12Envelope(doc_nsmap)
        # soapEnvelope.addBodyElement(bodyNode)
        # rep = self._mkNotificationReport(soapEnvelope, action)
        # try:
        #     roundtrip_timer = observableproperties.SingleValueCollector(self._soapClient, 'roundtrip_time')
        #
        #     self._soapClient.postSoapEnvelopeTo(self._url.path, rep, responseFactory=lambda x, schema: x,
        #                                         msg='send_notification_report {}'.format(action))
        #     try:
        #         roundtrip_time = roundtrip_timer.result(0)
        #         self.last_roundtrip_times.append(roundtrip_time)
        #         self.max_roundtrip_time = max(self.max_roundtrip_time, roundtrip_time)
        #     except observableproperties.TimeoutError:
        #         pass
        #     self._notifyErrors = 0
        #     self._isConnectionError = False
        # except soapclient.HTTPReturnCodeError:
        #     self._notifyErrors += 1
        #     raise
        # except Exception:  # any other exception is handled as an unreachable location (disconnected)
        #     self._notifyErrors += 1
        #     self._isConnectionError = True
        #     raise

    def sendNotificationEndMessage(self, action, code='SourceShuttingDown', reason='Event source going off line.'):
        pass
        # doc_nsmap = DocNamespaceHelper().docNssmap
        # my_addr = '{}:{}/{}'.format(self.base_urls[0].scheme, self.base_urls[0].netloc, self.base_urls[0].path)
        #
        # if not self.isValid:
        #     return
        # if self._soapClient is None:
        #     return
        # soapEnvelope = soapenvelope.Soap12Envelope(doc_nsmap)
        #
        # subscriptionEndNode = etree_.Element(wseTag('SubscriptionEnd'),
        #                                      nsmap=Prefix.partialMap(Prefix.WSE, Prefix.WSA, Prefix.XML))
        # subscriptionManagerNode = etree_.SubElement(subscriptionEndNode, wseTag('SubscriptionManager'))
        # # child of Subscriptionmanager is the endpoint reference of the subscription manager (wsa:EndpointReferenceType)
        # referenceParametersNode = etree_.Element(wsaTag('ReferenceParameters'))
        # referenceParametersNode.append(copy.copy(self.my_identifier))
        # epr = soapenvelope.WsaEndpointReferenceType(address=my_addr, referenceParametersNode=referenceParametersNode)
        # epr.asEtreeSubNode(subscriptionManagerNode)
        #
        # # remark: optionally one could add own address and identifier here ...
        # statusNode = etree_.SubElement(subscriptionEndNode, wseTag('Status'))
        # statusNode.text = 'wse:{}'.format(code)
        # reasonNode = etree_.SubElement(subscriptionEndNode, wseTag('Reason'),
        #                                attrib={xmlTag('lang'): 'en-US'})
        # reasonNode.text = reason
        #
        # soapEnvelope.addBodyElement(subscriptionEndNode)
        # rep = self._mkEndReport(soapEnvelope, action)
        # try:
        #     self._soapClient.postSoapEnvelopeTo(self._url.path, rep, responseFactory=lambda x, schema: x,
        #                                         msg='sendNotificationEndMessage {}'.format(action))
        #     self._notifyErrors = 0
        #     self._isConnectionError = False
        #     self._is_closed = True
        # except Exception:
        #     # it does not matter that we could not send the message - end is end ;)
        #     pass

    def close(self):
        self.reports.put('stop')
        self._is_closed = True

    def isClosed(self):
        return self._is_closed

    def __repr__(self):
        refIdent = '<unknown>'
        return 'Subscription(ref_idnt={}, my_idnt={}, expires={}, filter={})'.format(
            refIdent, self.my_identifier.text,  self.remainingSeconds,
            short_filter_string(self._filters))

    # @classmethod
    # def fromSoapEnvelope(cls, soapEnvelope, sslContext, bicepsSchema, acceptedEncodings, max_subscription_duration,
    #                      base_urls):
    #     endToAddress = None
    #     endToRefNode = []
    #     endToAddresses = soapEnvelope.bodyNode.xpath('wse:Subscribe/wse:EndTo', namespaces=nsmap)
    #     if len(endToAddresses) == 1:
    #         endToNode = endToAddresses[0]
    #         endToAddress = endToNode.xpath('wsa:Address/text()', namespaces=nsmap)[0]
    #         endToRefNode = endToNode.find('wsa:ReferenceParameters', namespaces=nsmap)
    #
    #     # determine (mandatory) notification address
    #     deliveryNode = soapEnvelope.bodyNode.xpath('wse:Subscribe/wse:Delivery', namespaces=nsmap)[0]
    #     notifyToNode = deliveryNode.find('wse:NotifyTo', namespaces=nsmap)
    #     notifyToAddress = notifyToNode.xpath('wsa:Address/text()', namespaces=nsmap)[0]
    #     notifyRefNode = notifyToNode.find('wsa:ReferenceParameters', namespaces=nsmap)
    #
    #     mode = deliveryNode.get('Mode')  # mandatory attribute
    #
    #     expiresNodes = soapEnvelope.bodyNode.xpath('wse:Subscribe/wse:Expires/text()', namespaces=nsmap)
    #     if len(expiresNodes) == 0:
    #         expires = None
    #     else:
    #         expires = isoduration.parse_duration(str(expiresNodes[0]))
    #
    #     filter_ = soapEnvelope.bodyNode.xpath('wse:Subscribe/wse:Filter/text()', namespaces=nsmap)[0]
    #
    #     return cls(str(mode), base_urls, notifyToAddress, notifyRefNode, endToAddress, endToRefNode,
    #                expires, max_subscription_duration, str(filter_), sslContext, bicepsSchema, acceptedEncodings)

    def get_roundtrip_stats(self):
        if len(self.last_roundtrip_times) > 0:
            return _RoundTripData(self.last_roundtrip_times, self.max_roundtrip_time)
        else:
            return _RoundTripData(None, None)

    def short_filter_names(self):
        return tuple([f.split('/')[-1] for f in self._filters])


class GSubscriptionsManager:

    DEFAULT_MAX_SUBSCR_DURATION = 7200  # max. possible duration of a subscription

    def __init__(self, sdc_definitions, max_subscription_duration=None, log_prefix=None):
        self.sdc_definitions = sdc_definitions
        self.log_prefix = log_prefix
        self._logger = loghelper.get_logger_adapter('sdc.grpc.dev.subscrMgr', self.log_prefix)
        #self.soapClients = {}  # key: net location, value soapClient instance
        self._max_subscription_duration = max_subscription_duration or self.DEFAULT_MAX_SUBSCR_DURATION
        self._subscriptions = multikey.MultiKeyLookup()
        self._subscriptions.add_index('identifier', multikey.UIndexDefinition(lambda obj: obj.my_identifier.text))
        self._subscriptions.add_index('netloc', multikey.IndexDefinition(
            lambda obj: obj._url.netloc))  # pylint:disable=protected-access
        self.base_urls = None

    def stop(self):
        for s in self._subscriptions.objects:
            s.close()

    def setBaseUrls(self, base_urls):
        self.base_urls = base_urls

    def on_subscribe_request(self, action_strings: list[str]) -> _GDevSubscription:
        s = _GDevSubscription(self._max_subscription_duration, action_strings)
        with self._subscriptions.lock:
            self._subscriptions.add_object(s)
        self._logger.info('new {}', s)
        return s

    def onUnsubscribeRequest(self, soapEnvelope):
        pass
        # ident = soapEnvelope.headerNode.find(_DevSubscription.IDENT_TAG, namespaces=nsmap)
        # if ident is not None:
        #     identtext = ident.text
        #     s = self._subscriptions.identifier.get_one(identtext, allowNone=True)
        #     if s is None:
        #         self._logger.warn('unsubscribe: no object found for id={}', identtext)
        #     else:
        #         s.close()
        #         with self._subscriptions.lock:
        #             self._subscriptions.removeObject(s)
        #         self._logger.info('unsubscribe: object found and removed (Xaddr = {}, filter = {})', s.notifyToAddress,
        #                           s._filters)  # pylint: disable=protected-access
        #         # now check if we can close the soap client
        #         key = s._url.netloc  # pylint: disable=protected-access
        #         subscriptionsWithSameSoapClient = self._subscriptions.netloc.get(key, [])
        #         if len(subscriptionsWithSameSoapClient) == 0:
        #             self.soapClients[key].close()
        #             del self.soapClients[key]
        #             self._logger.info('unsubscribe: closed soap client to {})', key)
        # else:
        #     self._logger.error('unsubscribe request did not contain an identifier!!!: {}',
        #                        soapEnvelope.as_xml(pretty=True))
        #
        # response = soapenvelope.Soap12Envelope(nsmap)
        # replyAddress = soapEnvelope.address.mkReplyAddress(
        #     'http://schemas.xmlsoap.org/ws/2004/08/eventing/UnsubscribeResponse')
        # response.addHeaderObject(replyAddress)
        # # response has empty body
        # return response

    # def notifyOperation(self, sequenceId, mdibVersion, transactionId, operation, invocation_state, error=None,
    #                     errorMessage=None):
    #     self._logger.info(
    #         'notifyOperation transaction={} operationHandleRef={}, operationState={}, error={}, errorMessage={}',
    #         transactionId, operation.handle, invocation_state, error, errorMessage)
    #     action = self.sdc_definitions.Actions.OperationInvokedReport
    #     subscribers = self._getSubscriptionsForAction(action)
    #     # if not subscribers:
    #     #     return
    #
    #     report = OperationInvokedReportStream()
    #     p_operation_invoked = report.operation_invoked
    #     self._set_mdib_version_group(p_operation_invoked.abstract_report.a_mdib_version_group, sequenceId, mdibVersion)
    #     p_report_part = p_operation_invoked.report_part.add()
    #     p_report_part.a_operation_handle_ref = operation.handle
    #     p_report_part.invocation_info.transaction_id = transactionId
    #     enum_attr_to_p(invocation_state, p_report_part.invocation_info.invocation_state)
    #     if error:
    #         enum_attr_to_p(error, p_report_part.invocation_info.invocation_error)
    #     if errorMessage:
    #         p_localized_text_msg = p_report_part.invocation_info.invocation_error_message.add() # LocalizedTextMsg
    #         p_localized_text_msg.string = errorMessage
    #     for s in subscribers:
    #         s.send_notification_report(report)




        # bodyNode = etree_.Element(msgTag('OperationInvokedReport'),
        #                           attrib={'SequenceId': sequenceId,
        #                                   'MdibVersion': str(mdibVersion)},
        #                           nsmap=Prefix.partialMap(Prefix.MSG, Prefix.PM))
        # reportPartNode = etree_.SubElement(bodyNode,
        #                                    msgTag('ReportPart'),
        #                                    attrib={'OperationHandleRef': operationHandleRef})
        # invocationInfoNode = etree_.SubElement(reportPartNode, msgTag('InvocationInfo'))
        # invocationSourceNode = etree_.SubElement(reportPartNode, msgTag('InvocationSource'),
        #                                          attrib={'Root': Prefix.SDC.namespace,
        #                                                  'Extension': 'AnonymousSdcParticipant'})
        # # implemented only SDC R0077 for value of invocationSourceNode:
        # # Root =  "http://standards.ieee.org/downloads/11073/11073-20701-2018"
        # # Extension = "AnonymousSdcParticipant".
        # # a known participant (R0078) is currently not supported
        # # ToDo: implement R0078
        # transactionIdNode = etree_.SubElement(invocationInfoNode, msgTag('TransactionId'))
        # transactionIdNode.text = str(transactionId)
        # operationStateNode = etree_.SubElement(invocationInfoNode, msgTag('InvocationState'))
        # operationStateNode.text = str(operationState)
        # if error is not None:
        #     errorNode = etree_.SubElement(invocationInfoNode, msgTag('InvocationError'))
        #     errorNode.text = str(error)
        # if errorMessage is not None:
        #     errorMessageNode = etree_.SubElement(invocationInfoNode, msgTag('InvocationErrorMessage'))
        #     errorMessageNode.text = str(errorMessage)
        #
        # for s in subscribers:
        #     self._logger.info('notifyOperation: sending report to {}', s.notifyToAddress)
        #     self._sendNotificationReport(s, bodyNode, action, Prefix.partialMap(Prefix.S12, Prefix.WSA, Prefix.WSE))
        # self._doHousekeeping()

    def onGetStatusRequest(self, soapEnvelope):
        raise NotImplementedError()
        self._logger.debug('onGetStatusRequest {}', lambda: soapEnvelope.as_xml(pretty=True))
        subscr = self._getSubscriptionforRequest(soapEnvelope)
        if subscr is None:
            response = soapenvelope.SoapFault(soapEnvelope,
                                              code='Receiver',
                                              reason='unknown Subscription identifier',
                                              subCode=wseTag('InvalidMessage')
                                              )

        else:
            response = soapenvelope.Soap12Envelope(Prefix.partialMap(Prefix.S12, Prefix.WSA, Prefix.WSE))
            replyAddress = soapEnvelope.address.mkReplyAddress(
                'http://schemas.xmlsoap.org/ws/2004/08/eventing/GetStatusResponse')
            response.addHeaderObject(replyAddress)
            renewResponseNode = etree_.Element(wseTag('GetStatusResponse'))
            expiresNode = etree_.SubElement(renewResponseNode, wseTag('Expires'))
            expiresNode.text = subscr.expireString  # simply confirm request
            response.addBodyElement(renewResponseNode)
        return response

    def onRenewRequest(self, soapEnvelope):
        raise NotImplementedError()
        identifierNode = soapEnvelope.headerNode.find(G_DevSubscription.IDENT_TAG, namespaces=nsmap)
        expires = soapEnvelope.bodyNode.xpath('wse:Renew/wse:Expires/text()', namespaces=nsmap)
        if len(expires) == 0:
            expires = None
            self._logger.debug('onRenewRequest: no requested duration found, allowing max. ',
                               lambda: soapEnvelope.as_xml(pretty=True))
        else:
            expires = isoduration.parse_duration(str(expires[0]))
            self._logger.debug('onRenewRequest {} seconds', expires)

        subscr = self._getSubscriptionforRequest(soapEnvelope)
        if subscr is None:
            response = soapenvelope.SoapFault(soapEnvelope,
                                              code='Receiver',
                                              reason='unknown Subscription identifier',
                                              subCode=wseTag('UnableToRenew')
                                              )

        else:
            subscr.renew(expires)

            response = soapenvelope.Soap12Envelope(Prefix.partialMap(Prefix.S12, Prefix.WSA, Prefix.WSE))
            replyAddress = soapEnvelope.address.mkReplyAddress(
                'http://schemas.xmlsoap.org/ws/2004/08/eventing/RenewResponse')
            response.addHeaderObject(replyAddress)
            renewResponseNode = etree_.Element(wseTag('RenewResponse'))
            expiresNode = etree_.SubElement(renewResponseNode, wseTag('Expires'))
            expiresNode.text = subscr.expireString
            response.addBodyElement(renewResponseNode)
        return response

    def send_episodic_metric_report(self, states, mdib_version_group):
        action = self.sdc_definitions.Actions.EpisodicMetricReport
        subscribers = self._getSubscriptionsForAction(action)
        if not subscribers:
            return
        self._logger.debug('sending episodic metric report {}', states)
        report = EpisodicReportStream()
        report.addressing.action = Actions.EpisodicMetricReport.value
        report.addressing.message_id = uuid.uuid4().urn
        oneof_report = report.report.metric
        mdib_version_group_msg = get_p_attr(oneof_report.abstract_metric_report.abstract_report,
                                            'MdibVersionGroup')
        set_mdib_version_group(mdib_version_group_msg, mdib_version_group)

        p_report_part = oneof_report.abstract_metric_report.report_part.add()
        for sc in states:
            p_st = p_report_part.metric_state.add()
            generic_state_to_p(sc, p_st)
        for s in subscribers:
            s.send_notification_report(report)

    def sendEpisodicOperationalStateReport(self, updatedStates, nsmapper, mdibVersion, sequenceId):
        action = self.sdc_definitions.Actions.EpisodicOperationalStateReport
        subscribers = self._getSubscriptionsForAction(action)
        if not subscribers:
            return
        self._logger.debug('sending episodic operational state report {}', updatedStates)
        report = EpisodicReportStream()
        report.addressing.action = Actions.EpisodicOperationalStateReport.value
        report.addressing.message_id = uuid.uuid4().urn

        oneof_report = report.report.operational_state
        self._set_mdib_version_group(oneof_report.abstract_operational_state_report.abstract_report.a_mdib_version_group, sequenceId, mdibVersion)
        for sc in updatedStates:
            p_report_part = oneof_report.abstract_operational_state_report.report_part.add()
            raise NotImplementedError()
            #p_st = p_report_part.
            #generic_state_to_p(sc, p_st)
        for s in subscribers:
            s.send_notification_report(report)

    def send_episodic_alert_report(self, states, mdib_version_group):
        action = self.sdc_definitions.Actions.EpisodicAlertReport
        subscribers = self._getSubscriptionsForAction(action)
        if not subscribers:
            return
        self._logger.debug('sending episodic alert report {}', states)
        report = EpisodicReportStream()
        report.addressing.action = Actions.EpisodicAlertReport.value
        report.addressing.message_id = uuid.uuid4().urn
        oneof_report = report.report.alert
        # self._set_mdib_version_group(oneof_report.abstract_alert_report.abstract_report.a_mdib_version_group, sequenceId, mdibVersion)
        mdib_version_group_msg = get_p_attr(oneof_report.abstract_alert_report.abstract_report,
                                            'MdibVersionGroup')
        set_mdib_version_group(mdib_version_group_msg, mdib_version_group)

        p_report_part = oneof_report.abstract_alert_report.report_part.add()
        for sc in states:
            p_st = p_report_part.alert_state.add()
            generic_state_to_p(sc, p_st)
        for s in subscribers:
            s.send_notification_report(report)

    def send_episodic_component_state_report(self, states, mdib_version_group):
        action = self.sdc_definitions.Actions.EpisodicComponentReport
        subscribers = self._getSubscriptionsForAction(action)
        if not subscribers:
            return
        self._logger.debug('sending episodic component report {}', states)
        report = EpisodicReportStream()
        report.addressing.action = Actions.EpisodicComponentReport.value
        report.addressing.message_id = uuid.uuid4().urn
        oneof_report = report.report.component
        mdib_version_group_msg = get_p_attr(oneof_report.abstract_component_report.abstract_report,
                                            'MdibVersionGroup')
        set_mdib_version_group(mdib_version_group_msg, mdib_version_group)
        for sc in states:
            p_report_part = oneof_report.abstract_component_report.report_part.add()
            p_st = p_report_part.component_state.add()
            generic_state_to_p(sc, p_st)
        for s in subscribers:
            s.send_notification_report(report)

    def send_episodic_context_report(self, states, mdib_version_group):
        action = self.sdc_definitions.Actions.EpisodicContextReport
        subscribers = self._getSubscriptionsForAction(action)
        if not subscribers:
            return
        report = EpisodicReportStream()
        report.addressing.action = Actions.EpisodicContextReport.value
        report.addressing.message_id = uuid.uuid4().urn
        oneof_report = report.report.context
        mdib_version_group_msg = get_p_attr(oneof_report.abstract_context_report.abstract_report,
                                            'MdibVersionGroup')
        set_mdib_version_group(mdib_version_group_msg, mdib_version_group)
        for sc in states:
            p_report_part = oneof_report.abstract_context_report.report_part.add() # ReportPartMsg
            p_st = p_report_part.context_state.add()
            generic_state_to_p(sc, p_st)
        for s in subscribers:
            s.send_notification_report(report)

    def sendRealtimeSamplesReport(self, updatedRealTimeSampleStates, nsmapper, mdibVersion, sequenceId):
        action = self.sdc_definitions.Actions.Waveform
        subscribers = self._getSubscriptionsForAction(action)
        if not subscribers:
            return
        self._logger.debug('sending real time samples report {}', updatedRealTimeSampleStates)
        report = EpisodicReportStream()
        report.addressing.action = Actions.Waveform.value
        report.addressing.message_id = uuid.uuid4().urn
        oneof_report = report.report.waveform
        self._set_mdib_version_group(oneof_report.abstract_report.a_mdib_version_group, sequenceId, mdibVersion)
        for sc in updatedRealTimeSampleStates:
            p_st = oneof_report.state.add()
            generic_state_to_p(sc, p_st)
        for s in subscribers:
            s.send_notification_report(report)

    def endAllSubscriptions(self, sendSubscriptionEnd):
        action = self.sdc_definitions.Actions.SubscriptionEnd
        with self._subscriptions.lock:
            if sendSubscriptionEnd:
                for s in self._subscriptions.objects:
                    s.sendNotificationEndMessage(action)
            self._subscriptions.clear()

    def _mkDescriptorUpdatesReportPart(self,
                                       report : DescriptionModificationReportMsg,
                                       modification_type: str, descriptors, updated_states):
        ''' Helper that creates ReportPart.'''
        # This method creates one ReportPart for every descriptor.
        # An optimization is possible by grouping all descriptors with the same parent handle into one ReportPart.
        # This is not implemented, and I think it is not needed.
        for descr_container in descriptors:
            p_report_part = report.report_part.add()
            # map = {'Crt': 0, 'Upt': 1, 'Del': 2}
            # p_report_part.a_modification_type.enum_value = map[modification_type]
            enum_attr_to_p(modification_type, get_p_attr(p_report_part, 'ModificationType'))
            if descr_container.parent_handle is not None:  # only Mds can have None
                get_p_attr(p_report_part, 'ParentDescriptor').string = descr_container.parent_handle
                # p_report_part.a_parent_descriptor.value = descrContainer.parentHandle

            p_descr = p_report_part.p_descriptor.add()
            generic_descriptor_to_p(descr_container, p_descr)
            related_state_containers = [s for s in updated_states if s.DescriptorHandle == descr_container.Handle]
            for state_container in related_state_containers:
                p_state = p_report_part.state.add()
                generic_state_to_p(state_container, p_state)

                #reportPart.set('ParentDescriptor', descrContainer.parentHandle)
            # node = descrContainer.mkDescriptorNode(tag=msgTag('Descriptor'))
            # reportPart.append(node)
            # relatedStateContainers = [s for s in updated_states if s.descriptorHandle == descrContainer.handle]
            # for stateContainer in relatedStateContainers:
            #     node = stateContainer.mkStateNode(msgTag('State'))
            #     reportPart.append(node)

    def send_descriptor_updates(self, updated, created, deleted, updated_states, mdib_version_group):
        action = self.sdc_definitions.Actions.DescriptionModificationReport
        subscribers = self._getSubscriptionsForAction(action)
        if not subscribers:
            return
        self._logger.debug('sending DescriptionModificationReport upd={} crt={} del={}', updated, created, deleted)
        report = EpisodicReportStream()
        oneof_report = report.report.description
        mdib_version_group_msg = get_p_attr(oneof_report.abstract_report,
                                            'MdibVersionGroup')
        set_mdib_version_group(mdib_version_group_msg, mdib_version_group)

        # self._set_mdib_version_group(oneof_report.abstract_report.a_mdib_version_group, sequenceId, mdibVersion)
        # bodyNode = etree_.Element(msgTag('DescriptionModificationReport'),
        #                           attrib={'SequenceId': sequenceId,
        #                                   'MdibVersion': str(mdibVersion)},
        #                           nsmap=Prefix.partialMap(Prefix.MSG, Prefix.PM))
        self._mkDescriptorUpdatesReportPart(oneof_report, 'Upt', updated, updated_states)
        self._mkDescriptorUpdatesReportPart(oneof_report, 'Crt', created, updated_states)
        self._mkDescriptorUpdatesReportPart(oneof_report, 'Del', deleted, updated_states)

        for s in subscribers:
            s.send_notification_report(report)


    # def send_operation_invoked_report(self, report):
    #     report = EpisodicReportStream()
    #
    #     action = self.sdc_definitions.Actions.OperationInvokedReport
    #     subscribers = self._getSubscriptionsForAction(action)
    #     if not subscribers:
    #         return
    #     self._logger.debug('sending OperationInvokedReport to %d subscribers', len(subscribers))
    #     for s in subscribers:
    #         s.send_notification_report(report)

    def send_operation_invoked_report(self,
                         operation: OperationDefinition,
                         transaction_id: int,
                         invocation_state: Enum,
                         mdib_version_group: MdibVersionGroup,
                         operation_target: str | None = None,
                         error: Enum | None = None,
                         error_message: str | None = None):
        action = self.sdc_definitions.Actions.OperationInvokedReport
        subscribers = self._getSubscriptionsForAction(action)
        if not subscribers:
            return

        # episodic_report = EpisodicReportStream()
        # report = episodic_report.operation_invoked_report
        stream = OperationInvokedReportStream()
        stream.addressing.action = action.value
        stream.addressing.message_id = 'bernd'
        op_invoked = stream.operation_invoked
        mdib_version_group_msg = get_p_attr(op_invoked.abstract_report, 'MdibVersionGroup')

        set_mdib_version_group(mdib_version_group_msg, mdib_version_group)
        report_part = op_invoked.report_part.add()
        report_part.invocation_info.transaction_id.unsigned_int = transaction_id
        enum_attr_to_p(invocation_state, report_part.invocation_info.invocation_state)
        if error is not None:
            enum_attr_to_p(error, report_part.invocation_info.invocation_error)
        if error_message is not None:
            tmp = report_part.invocation_info.invocation_error_message.add()  # LocalizedTextMsg
            tmp.localized_text_content.string = error_message
            get_p_attr(tmp, 'Lang').value = 'EN_en'
        get_p_attr(report_part.invocation_source.instance_identifier, 'Root').any_u_r_i = nsh.SDC.namespace
        get_p_attr(report_part.invocation_source.instance_identifier, 'Extension').string = 'AnonymousSdcParticipant'
        get_p_attr(report_part, 'OperationHandleRef').string = operation.handle
        if operation_target is not None:
            get_p_attr(report_part, 'OperationTarget').string = operation_target

        for s in subscribers:
            s.send_notification_report(stream)

    # def _sendNotificationReport(self, subscription, bodyNode, action, doc_nsmap):
    #     try:
    #         subscription.send_notification_report(bodyNode, action, doc_nsmap)
    #     except soapclient.HTTPReturnCodeError as ex:
    #         # this is an error related to the connection => log error and continue
    #         self._logger.error('could not send notification report: HTTP status= {}, reason={}, {}', ex.status,
    #                            ex.reason, subscription)
    #     except http.client.NotConnected as ex:
    #         # this is an error related to the connection => log error and continue
    #         self._logger.error('could not send notification report: {!r}:  subscr = {}', ex, subscription)
    #     except socket.timeout as ex:
    #         # this is an error related to the connection => log error and continue
    #         self._logger.error('could not send notification report error= {!r}: {}', ex, subscription)
    #     except etree_.DocumentInvalid as ex:
    #         # this is an error related to the document, it cannot be sent to any subscriber => re-raise
    #         self._logger.error('Invalid Document: {!r}\n{}', ex, etree_.tostring(bodyNode))
    #         raise
    #     except Exception as ex:
    #         # this should never happen! => re-raise
    #         self._logger.error('could not send notification report error= {!r}: {}', ex, subscription)

    def _getSubscriptionsForAction(self, action):
        with self._subscriptions.lock:
            return [s for s in self._subscriptions.objects if s.matches(action)]

    def _getSubscriptionforRequest(self, soapEnvelope):
        request_name = soapEnvelope.bodyNode[0].tag
        identifierNode = soapEnvelope.headerNode.find(_GDevSubscription.IDENT_TAG, namespaces=nsmap)
        if identifierNode is None:
            raise RuntimeError('no Identifier found in {} ', request_name)
        else:
            identifier = identifierNode.text
        with self._subscriptions.lock:
            subscr = [s for s in self._subscriptions.objects if s.my_identifier.text == identifier]
        if len(subscr) > 1:
            raise RuntimeError('Have {} subscriptions with identifier "{}"!'.format(len(subscr), identifier))
        elif len(subscr) == 0:
            self._logger.error('on {}: unknown Subscription identifier "{}"', request_name, identifier)
            return
        return subscr[0]

    def _doHousekeeping(self):
        ''' remove expired or invalid subscriptions'''
        with self._subscriptions._lock:  # pylint: disable=protected-access
            crap = [s for s in self._subscriptions.objects if not s.isValid]
        unreachable_netlocs = []
        for c in crap:
            if c.hasConnectionError:
                # the network location is unreachable, we can remove all subscriptions that use this location
                unreachable_netlocs.append(c.soapClient.netloc)
                try:
                    c.soapClient.close()
                except:
                    self._logger.error('error in soapClient.close(): {}', traceback.format_exc())

            self._logger.info('deleting {}, errors={}', c, c._notifyErrors)  # pylint: disable=protected-access
            with self._subscriptions.lock:
                self._subscriptions.removeObject(c)

            if c.soapClient.netloc in self.soapClients:  # remove closed soap client from list
                del self.soapClients[c.soapClient.netloc]

        # now find all subscriptions that have the same address
        with self._subscriptions._lock:  # pylint: disable=protected-access
            also_unreachable = [s for s in self._subscriptions.objects if
                                s.soapClient is not None and s.soapClient.netloc in unreachable_netlocs]
            for s in also_unreachable:
                self._logger.info('deleting also subscription {}, same endpoint', s)
                self._subscriptions.removeObject(s)

    def getSubScriptionRoundtripTimes(self):
        '''Calculates roundtrip times based on last MAX_ROUNDTRIP_VALUES values.

        @return: a dictionary with key=(<notifyToAddress>, (subscriptionnames)), value = _RoundTripData with members min, max, avg, abs_max, values
        '''
        ret = {}
        with self._subscriptions.lock:
            for s in self._subscriptions.objects:
                if s.max_roundtrip_time > 0:
                    ret[(s.notifyToAddress, s.short_filter_names())] = s.get_roundtrip_stats()
        return ret

    def getClientRoundtripTimes(self):
        '''Calculates roundtrip times based on last MAX_ROUNDTRIP_VALUES values.

        @return: a dictionary with key=<notifyToAddress>, value = _RoundTripData with members min, max, avg, abs_max, values
        '''
        # first step: collect all roundtrip times of subscriptions, group them by notifyToAddress
        tmp = defaultdict(list)
        ret = {}
        with self._subscriptions.lock:
            for s in self._subscriptions.objects:
                if s.max_roundtrip_time > 0:
                    tmp[s.notifyToAddress].append(s.get_roundtrip_stats())
        for k, stats in tmp.items():
            allvalues = []
            for s in stats:
                allvalues.extend(s.values)
            ret[k] = _RoundTripData(allvalues, max([s.max for s in stats]), )
        return ret

