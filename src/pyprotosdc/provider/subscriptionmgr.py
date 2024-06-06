from __future__ import annotations
from typing import TYPE_CHECKING
import uuid
import time
from collections import deque, defaultdict
import queue
from lxml import etree as etree_
from sdc11073.etc import short_filter_string
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


class GDevSubscription(object):
    MAX_NOTIFY_ERRORS = 1
    IDENT_TAG = etree_.QName('http.local.com', 'MyDevIdentifier')

    def __init__(self, max_subscription_duration, filter_):  # pylint:disable=too-many-arguments
        self.my_identifier = etree_.Element(self.IDENT_TAG)
        self.my_identifier.text = uuid.uuid4().urn

        self._max_subscription_duration = max_subscription_duration
        # self._started = None
        # self._expireseconds = None
        # self.renew(7200)  # sets self._started and self._expireseconds
        self._filters = filter_

        self._notifyErrors = 0
        self._is_closed = False
        self._isConnectionError = False
        self.last_roundtrip_times = deque(
            maxlen=MAX_ROUNDTRIP_VALUES)  # a list of last n roundtrip times for notifications
        self.max_roundtrip_time = 0
        self.reports = queue.Queue(maxsize=50)

    # def renew(self, expires):
    #     self._started = time.monotonic()
    #     if expires:
    #         self._expireseconds = min(expires, self._max_subscription_duration)
    #     else:
    #         self._expireseconds = self._max_subscription_duration

    # @property
    # def remainingSeconds(self):
    #     duration = int(self._expireseconds - (time.monotonic() - self._started))
    #     return 0 if duration < 0 else duration
    #
    # @property
    # def hasDeliveryFailure(self):
    #     return self._notifyErrors >= self.MAX_NOTIFY_ERRORS

    # @property
    # def hasConnectionError(self):
    #     return self._isConnectionError

    # @property
    # def isValid(self):
    #     if self._is_closed:
    #         return False
    #     return self.remainingSeconds > 0 and not self.hasDeliveryFailure

    def matches(self, action):
        action = action.strip()  # just to be sure there are no spaces....
        for f in self._filters:
            if f.endswith(action):
                return True
        return False

    def send_notification_report(self, report):
        self.reports.put(report)

    # def sendNotificationEndMessage(self, action, code='SourceShuttingDown', reason='Event source going off line.'):
    #     pass

    def close(self):
        self.reports.put('stop')
        self._is_closed = True

    # def isClosed(self):
    #     return self._is_closed

    def __repr__(self):
        refIdent = '<unknown>'
        return 'Subscription(ref_idnt={}, my_idnt={}, filter={})'.format(
            refIdent, self.my_identifier.text,
            short_filter_string(self._filters))

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
        self._max_subscription_duration = max_subscription_duration or self.DEFAULT_MAX_SUBSCR_DURATION
        self._subscriptions = multikey.MultiKeyLookup()
        self._subscriptions.add_index('identifier', multikey.UIndexDefinition(lambda obj: obj.my_identifier.text))
        self._subscriptions.add_index('netloc', multikey.IndexDefinition(
            lambda obj: obj._url.netloc))  # pylint:disable=protected-access
        self.base_urls = None

    def stop(self):
        for s in self._subscriptions.objects:
            s.close()


    def on_subscribe_request(self, action_strings: list[str]) -> GDevSubscription:
        s = GDevSubscription(self._max_subscription_duration, action_strings)
        with self._subscriptions.lock:
            self._subscriptions.add_object(s)
        self._logger.info('new {}', s)
        return s

    def remove_subscription(self, subscription: GDevSubscription):
        self._logger.info('remove  {}', subscription)
        self._subscriptions.remove_object(subscription)
        # read queue empty in order to avoid possible blockings
        while not subscription.reports.empty():
            subscription.reports.get()

    def send_episodic_metric_report(self, states, mdib_version_group):
        action = self.sdc_definitions.Actions.EpisodicMetricReport
        subscribers = self._getSubscriptionsForAction(action)
        if not subscribers:
            self._logger.info('sending episodic metric report: no subscribers')
            return
        self._logger.info('sending episodic metric report to %d subscribers', len(subscribers))
        report = EpisodicReportStream()
        report.addressing.action = Actions.EpisodicMetricReport.value
        report.addressing.message_id = uuid.uuid4().urn
        oneof_report = report.report.metric
        mdib_version_group_msg = get_p_attr(oneof_report.abstract_metric_report.abstract_report,
                                            'MdibVersionGroup')
        set_mdib_version_group(mdib_version_group_msg, mdib_version_group)

        p_report_part = oneof_report.abstract_metric_report.report_part.add()
        p_report_part.abstract_report_part.source_mds.string = 'ToDo'
        for sc in states:
            p_st = p_report_part.metric_state.add()
            generic_state_to_p(sc, p_st)
        for s in subscribers:
            s.send_notification_report(report)

    def send_episodic_operational_state_report(self, states, mdib_version_group):
        action = self.sdc_definitions.Actions.EpisodicOperationalStateReport
        subscribers = self._getSubscriptionsForAction(action)
        if not subscribers:
            self._logger.info('sending episodic operational state report: no subscribers')
            return
        self._logger.info('sending episodic operational state report to %d subscribers', len(subscribers))
        report = EpisodicReportStream()
        report.addressing.action = Actions.EpisodicOperationalStateReport.value
        report.addressing.message_id = uuid.uuid4().urn
        oneof_report = report.report.operational_state
        mdib_version_group_msg = get_p_attr(oneof_report.abstract_operational_state_report.abstract_report,
                                            'MdibVersionGroup')
        set_mdib_version_group(mdib_version_group_msg, mdib_version_group)
        p_report_part = oneof_report.abstract_operational_state_report.report_part.add()
        p_report_part.abstract_report_part.source_mds.string = 'ToDo'
        for sc in states:
            p_st = p_report_part.operation_state.add()
            generic_state_to_p(sc, p_st)
        for s in subscribers:
            s.send_notification_report(report)

    def send_episodic_alert_report(self, states, mdib_version_group):
        action = self.sdc_definitions.Actions.EpisodicAlertReport
        subscribers = self._getSubscriptionsForAction(action)
        if not subscribers:
            self._logger.info('sending episodic alert report: no subscribers')
            return
        self._logger.info('sending episodic alert report to %d subscribers', len(subscribers))
        report = EpisodicReportStream()
        report.addressing.action = Actions.EpisodicAlertReport.value
        report.addressing.message_id = uuid.uuid4().urn
        oneof_report = report.report.alert
        mdib_version_group_msg = get_p_attr(oneof_report.abstract_alert_report.abstract_report,
                                            'MdibVersionGroup')
        set_mdib_version_group(mdib_version_group_msg, mdib_version_group)

        p_report_part = oneof_report.abstract_alert_report.report_part.add()
        p_report_part.abstract_report_part.source_mds.string = 'ToDo'
        for sc in states:
            p_st = p_report_part.alert_state.add()
            generic_state_to_p(sc, p_st)
        for s in subscribers:
            s.send_notification_report(report)

    def send_episodic_component_state_report(self, states, mdib_version_group):
        action = self.sdc_definitions.Actions.EpisodicComponentReport
        subscribers = self._getSubscriptionsForAction(action)
        if not subscribers:
            self._logger.info('sending episodic component state report: no subscribers')
            return
        self._logger.info('sending episodic component state report to %d subscribers', len(subscribers))
        report = EpisodicReportStream()
        report.addressing.action = Actions.EpisodicComponentReport.value
        report.addressing.message_id = uuid.uuid4().urn
        oneof_report = report.report.component
        mdib_version_group_msg = get_p_attr(oneof_report.abstract_component_report.abstract_report,
                                            'MdibVersionGroup')
        set_mdib_version_group(mdib_version_group_msg, mdib_version_group)
        for sc in states:
            p_report_part = oneof_report.abstract_component_report.report_part.add()
            p_report_part.abstract_report_part.source_mds.string = 'ToDo'
            p_st = p_report_part.component_state.add()
            generic_state_to_p(sc, p_st)
        for s in subscribers:
            s.send_notification_report(report)

    def send_episodic_context_report(self, states, mdib_version_group):
        action = self.sdc_definitions.Actions.EpisodicContextReport
        subscribers = self._getSubscriptionsForAction(action)
        if not subscribers:
            self._logger.info('sending episodic context state report: no subscribers')
            return
        self._logger.info('sending episodic context state report to %d subscribers', len(subscribers))
        report = EpisodicReportStream()
        report.addressing.action = Actions.EpisodicContextReport.value
        report.addressing.message_id = uuid.uuid4().urn
        oneof_report = report.report.context
        mdib_version_group_msg = get_p_attr(oneof_report.abstract_context_report.abstract_report,
                                            'MdibVersionGroup')
        set_mdib_version_group(mdib_version_group_msg, mdib_version_group)
        for sc in states:
            p_report_part = oneof_report.abstract_context_report.report_part.add() # ReportPartMsg
            p_report_part.abstract_report_part.source_mds.string = 'ToDo'
            p_st = p_report_part.context_state.add()
            generic_state_to_p(sc, p_st)
        for s in subscribers:
            s.send_notification_report(report)

    def send_realtime_samples_report(self, states, mdib_version_group):
        action = self.sdc_definitions.Actions.Waveform
        subscribers = self._getSubscriptionsForAction(action)
        if not subscribers:
            # self._logger.info('sending real time samples report: no subscribers')
            return
        self._logger.info('sending real time samples report to %d subscribers', len(subscribers))
        episodic_report_stream = EpisodicReportStream()
        episodic_report_stream.addressing.action = Actions.Waveform.value
        episodic_report_stream.addressing.message_id = uuid.uuid4().urn
        waveform_stream_msg = episodic_report_stream.report.waveform
        mdib_version_group_msg = get_p_attr(waveform_stream_msg.abstract_report,
                                            'MdibVersionGroup')
        set_mdib_version_group(mdib_version_group_msg, mdib_version_group)
        for sc in states:
            p_st = waveform_stream_msg.state.add()
            generic_state_to_p(sc, p_st)
        for s in subscribers:
            s.send_notification_report(episodic_report_stream)

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
        """ Helper that creates ReportPart."""
        # This method creates one ReportPart for every descriptor.
        # An optimization is possible by grouping all descriptors with the same parent handle into one ReportPart.
        # This is not implemented, and I think it is not needed.
        for descr_container in descriptors:
            p_report_part = report.report_part.add()
            p_report_part.abstract_report_part.source_mds.string = 'ToDo'
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

    def send_descriptor_updates(self, updated, created, deleted, updated_states, mdib_version_group):
        action = self.sdc_definitions.Actions.DescriptionModificationReport
        subscribers = self._getSubscriptionsForAction(action)
        if not subscribers:
            self._logger.info('sending DescriptionModificationReport: no subscribers')
            return
        self._logger.info('sending DescriptionModificationReport upd={} crt={} del={}', updated, created, deleted)
        report = EpisodicReportStream()
        oneof_report = report.report.description
        mdib_version_group_msg = get_p_attr(oneof_report.abstract_report,
                                            'MdibVersionGroup')
        set_mdib_version_group(mdib_version_group_msg, mdib_version_group)

        self._mkDescriptorUpdatesReportPart(oneof_report, 'Upt', updated, updated_states)
        self._mkDescriptorUpdatesReportPart(oneof_report, 'Crt', created, updated_states)
        self._mkDescriptorUpdatesReportPart(oneof_report, 'Del', deleted, updated_states)

        for s in subscribers:
            s.send_notification_report(report)

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
            self._logger.info('sending operation invoked: no subscribers')
            return
        self._logger.info('sending operation invoked to %d subscribers', len(subscribers))

        stream = OperationInvokedReportStream()
        stream.addressing.action = action.value
        stream.addressing.message_id = 'bernd'
        op_invoked = stream.operation_invoked
        mdib_version_group_msg = get_p_attr(op_invoked.abstract_report, 'MdibVersionGroup')

        set_mdib_version_group(mdib_version_group_msg, mdib_version_group)
        report_part = op_invoked.report_part.add()
        report_part.abstract_report_part.source_mds.string = 'ToDo'
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

    def _getSubscriptionsForAction(self, action):
        with self._subscriptions.lock:
            return [s for s in self._subscriptions.objects if s.matches(action)]

    def _getSubscriptionforRequest(self, soapEnvelope):
        request_name = soapEnvelope.bodyNode[0].tag
        identifierNode = soapEnvelope.headerNode.find(GDevSubscription.IDENT_TAG, namespaces=nsmap)
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

    #
    # def getSubScriptionRoundtripTimes(self):
    #     '''Calculates roundtrip times based on last MAX_ROUNDTRIP_VALUES values.
    #
    #     @return: a dictionary with key=(<notifyToAddress>, (subscriptionnames)), value = _RoundTripData with members min, max, avg, abs_max, values
    #     '''
    #     ret = {}
    #     with self._subscriptions.lock:
    #         for s in self._subscriptions.objects:
    #             if s.max_roundtrip_time > 0:
    #                 ret[(s.notifyToAddress, s.short_filter_names())] = s.get_roundtrip_stats()
    #     return ret
    #
    # def getClientRoundtripTimes(self):
    #     '''Calculates roundtrip times based on last MAX_ROUNDTRIP_VALUES values.
    #
    #     @return: a dictionary with key=<notifyToAddress>, value = _RoundTripData with members min, max, avg, abs_max, values
    #     '''
    #     # first step: collect all roundtrip times of subscriptions, group them by notifyToAddress
    #     tmp = defaultdict(list)
    #     ret = {}
    #     with self._subscriptions.lock:
    #         for s in self._subscriptions.objects:
    #             if s.max_roundtrip_time > 0:
    #                 tmp[s.notifyToAddress].append(s.get_roundtrip_stats())
    #     for k, stats in tmp.items():
    #         allvalues = []
    #         for s in stats:
    #             allvalues.extend(s.values)
    #         ret[k] = _RoundTripData(allvalues, max([s.max for s in stats]), )
    #     return ret

