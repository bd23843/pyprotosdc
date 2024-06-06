from __future__ import annotations

import time
from typing import TYPE_CHECKING

from sdc11073 import observableproperties as properties
from sdc11073.mdib import consumermdib
from sdc11073.xml_types import msg_types

from pyprotosdc.mapping.basic_mappers import enum_attr_from_p
from pyprotosdc.mapping.mapping_helpers import get_p_attr
from pyprotosdc.actions import ReportAction

if TYPE_CHECKING:
    from pyprotosdc.consumer.serviceclients.mdibreportingservice import EpisodicReportData
    from pyprotosdc.consumer.consumer import GSdcConsumer

LOG_WF_AGE_INTERVAL = 30  # how often a log message is written with mean and standard-deviation of waveforms age
AGE_CALC_SAMPLES_COUNT = 100  # amount of data for wf mean age and standard-deviation calculation

A_NO_LOG = 0
A_OUT_OF_RANGE = 1
A_STILL_OUT_OF_RANGE = 2
A_BACK_IN_RANGE = 3


class GClientMdibContainer(consumermdib.ConsumerMdib):

    def __init__(self,
                 sdc_consumer: GSdcConsumer,
                 extras_cls: type | None = None,
                 max_realtime_samples: int = 100):
        super().__init__(sdc_consumer, extras_cls, max_realtime_samples)

    def init_mdib(self):
        if self._is_initialized:
            raise RuntimeError('ClientMdibContainer is already initialized')
        # first start receiving notifications, then call getMdib.
        # Otherwise we might miss notifications.
        self._bind_to_observables()

        cl_get_service = self._sdc_client.get_service
        self._logger.info('initializing mdib...')
        response = cl_get_service.get_mdib()
        self._logger.info('creating description containers...')
        with self.descriptions._lock:  # pylint: disable=protected-access
            self.descriptions.clear()
        self.add_description_containers(response.descriptors)
        self._logger.info('creating state containers...')
        self.add_state_containers(response.states)

        mdib_version_group = response.mdib_version_group
        if mdib_version_group.mdib_version is not None:
            self.mdib_version = mdib_version_group.mdib_version
            self._logger.info('setting initial mdib version to {}', mdib_version_group.mdib_version)  # noqa: PLE1205
        else:
            self._logger.warning('found no mdib version in GetMdib response, assuming "0"')
            self.mdib_version = 0
        self.sequence_id = mdib_version_group.sequence_id
        self._logger.info('setting initial sequence id to {}', mdib_version_group.sequence_id)  # noqa: PLE1205
        if mdib_version_group.instance_id != self.instance_id:
            self.instance_id = mdib_version_group.instance_id
        self._logger.info('setting initial instance id to {}', mdib_version_group.instance_id)  # noqa: PLE1205

        # process buffered notifications
        with self._buffered_notifications_lock:
            for buffered_report in self._buffered_notifications:
                buffered_report.handler(buffered_report.report, is_buffered_report=True)
            del self._buffered_notifications[:]
            self._is_initialized = True
        self._logger.info('initializing mdib done')

    def _bind_to_observables(self):
        # observe properties of sdcClient
        properties.bind(self._sdc_client, any_report=self._on_any_report)

    def _on_any_report(self, report: EpisodicReportData, is_buffered_report=False):
        handler_lookup = {ReportAction.Waveform: self._on_waveform_report,
                          ReportAction.EpisodicMetricReport: self._on_episodic_metric_report,
                          ReportAction.EpisodicAlertReport: self._on_episodic_alert_report,
                          ReportAction.EpisodicComponentReport: self._on_episodic_component_report,
                          ReportAction.EpisodicContextReport: self._on_episodic_context_report,
                          ReportAction.DescriptionModificationReport: self._on_description_modification_report,
                          ReportAction.EpisodicOperationalStateReport: self._on_episodic_operational_state_report
                          }
        self.logger.debug('received report %s', report.action)
        handler = handler_lookup[report.action]

        if not self._can_accept_mdib_version('_on_any_report', report.mdib_version_group.mdib_version):
            self._logger.warn('ignoring %s, mdib version too old (got %d, expect %d)',
                              report.action, report.mdib_version_group.mdib_version, self.mdib_version + 1)
            return
        try:
            return handler(report, is_buffered_report)
        except Exception as ex:
            raise

    def _on_episodic_metric_report(self, report_data: EpisodicReportData, is_buffered_report):
        now = time.time()
        metrics_by_handle = {}
        max_age = 0
        min_age = 0
        state_containers = []
        report = report_data.p_response.report.metric
        for report_part in report.abstract_metric_report.report_part:
            state_containers.extend(report_data.msg_reader.read_states(report_part.metric_state, self))
        try:
            with self.mdib_lock:
                self.mdib_version = report_data.mdib_version_group.mdib_version
                if report_data.mdib_version_group.sequence_id != self.sequence_id:
                    self.sequence_id = report_data.mdib_version_group.sequence_id
                for sc in state_containers:
                    if sc.descriptor_container is not None and \
                            sc.descriptor_container.DescriptorVersion != sc.DescriptorVersion:
                        self._logger.warn(
                            '_onEpisodicMetricReport: metric "{}": descriptor version expect "{}", found "{}"',
                            sc.descriptorHandle, sc.DescriptorVersion, sc.descriptor_container.DescriptorVersion)
                        sc.descriptor_container = None
                    try:
                        old_state_container = self.states.descriptor_handle.get_one(sc.DescriptorHandle,
                                                                                    allow_none=True)
                    except RuntimeError as ex:
                        self._logger.error('_onEpisodicMetricReport, get_one on states: {}', ex)
                        continue
                    desc_h = sc.DescriptorHandle
                    metrics_by_handle[desc_h] = sc  # metric
                    if old_state_container is not None:
                        if self._has_new_state_usable_state_version(old_state_container, sc, 'EpisodicMetricReport',
                                                                    is_buffered_report):
                            old_state_container.update_from_other_container(sc)
                            self.states.update_object(old_state_container)
                    else:
                        self.states.add_object(sc)

                    if sc.MetricValue is not None:
                        observation_time = sc.MetricValue.DeterminationTime
                        if observation_time is None:
                            self._logger.warn(
                                '_onEpisodicMetricReport: metric {} version {} has no DeterminationTime',
                                desc_h, sc.StateVersion)
                        else:
                            age = now - observation_time
                            min_age = min(min_age, age)
                            max_age = max(max_age, age)
            # shall_log = self.metric_time_warner.getOutOfDeterminationTimeLogState(
            #     min_age, max_age, self.DETERMINATIONTIME_WARN_LIMIT)
            # if shall_log == A_OUT_OF_RANGE:
            #     self._logger.warn(
            #         '_onEpisodicMetricReport mdibVersion {}: age of metrics outside limit of {} sec.: '
            #         'max, min = {:03f}, {:03f}',
            #         new_mdib_version, self.DETERMINATIONTIME_WARN_LIMIT, max_age, min_age)
            # elif shall_log == A_STILL_OUT_OF_RANGE:
            #     self._logger.warn(
            #         '_onEpisodicMetricReport mdibVersion {}: age of metrics still outside limit of {} sec.: '
            #         'max, min = {:03f}, {:03f}',
            #         new_mdib_version, self.DETERMINATIONTIME_WARN_LIMIT, max_age, min_age)
            # elif shall_log == A_BACK_IN_RANGE:
            #     self._logger.info(
            #         '_onEpisodicMetricReport mdibVersion {}: age of metrics back in limit of {} sec.: '
            #         'max, min = {:03f}, {:03f}',
            #         new_mdib_version, self.DETERMINATIONTIME_WARN_LIMIT, max_age, min_age)
        finally:
            self.metrics_by_handle = metrics_by_handle  # used by waitMetricMatches method

    def _on_episodic_alert_report(self, report_data: EpisodicReportData, is_buffered_report):
        alert_by_handle = {}
        report = report_data.p_response.report
        try:
            state_containers = []
            for report_part in report.alert.abstract_alert_report.report_part:
                state_containers.extend(report_data.msg_reader.read_states(report_part.alert_state, self))
            with self.mdib_lock:
                self.mdib_version = report_data.mdib_version_group.mdib_version
                if report_data.mdib_version_group.sequence_id != self.sequence_id:
                    self.sequence_id = report_data.mdib_version_group.sequence_id
                for sc in state_containers:
                    if sc.descriptor_container is not None and \
                            sc.descriptor_container.DescriptorVersion != sc.DescriptorVersion:
                        self._logger.warn(
                            '_on_episodic_alert_report: alert "{}": descriptor version expect "{}", found "{}"',
                            sc.descriptorHandle, sc.DescriptorVersion, sc.descriptor_container.DescriptorVersion)
                        sc.descriptor_container = None
                    try:
                        old_state_container = self.states.descriptor_handle.get_one(sc.DescriptorHandle,
                                                                                    allow_none=True)
                    except RuntimeError as ex:
                        self._logger.error('_onEpisodicAlertReport, get_one on states: {}', ex)
                        continue
                    if old_state_container is not None:
                        if self._has_new_state_usable_state_version(old_state_container, sc, 'EpisodicAlertReport',
                                                                    is_buffered_report):
                            old_state_container.update_from_other_container(sc)
                            self.states.update_object(old_state_container)
                            alert_by_handle[old_state_container.DescriptorHandle] = old_state_container
                    else:
                        self.states.add_object(sc)
                        alert_by_handle[sc.DescriptorHandle] = sc
        finally:
            self.alert_by_handle = alert_by_handle  # update observable

    def _on_waveform_report(self, report_data: EpisodicReportData, is_buffered_report):
        report = report_data.p_response.report
        waveform_by_handle = {}
        waveform_age = {}  # collect age of all waveforms in this report,
        # and make one report if age is above warn limit (instead of multiple)
        try:
            all_states = report_data.msg_reader.read_states(report.waveform.state, self)

            with self.mdib_lock:
                self.mdib_version = report_data.mdib_version_group.mdib_version
                if report_data.mdib_version_group.sequence_id != self.sequence_id:
                    self.sequence_id = report_data.mdib_version_group.sequence_id
                for new_sac in all_states:
                    d_handle = new_sac.DescriptorHandle
                    descriptor_container = new_sac.descriptor_container
                    if descriptor_container is None:
                        self._logger.warn('_onWaveformReport: No Descriptor found for handle "{}"', d_handle)

                    old_state_container = self.states.descriptor_handle.get_one(d_handle, allow_none=True)
                    if old_state_container is None:
                        self.states.add_object(new_sac)
                        current_sc = new_sac
                    else:
                        if self._has_new_state_usable_state_version(old_state_container, new_sac,
                                                                    'WaveformReport', is_buffered_report):
                            # update old state container from new one
                            old_state_container.update_from_other_container(new_sac)
                            self.states.update_object(old_state_container)
                        current_sc = old_state_container  # we will need it later
                    waveform_by_handle[d_handle] = current_sc
                    # add to Waveform Buffer
                    rt_buffer = self.rt_buffers.get(d_handle)
                    if rt_buffer is None:
                        if descriptor_container is not None:
                            # read sample period
                            try:
                                sample_period = descriptor_container.SamplePeriod or 0
                            except AttributeError:
                                sample_period = 0  # default
                        rt_buffer = consumermdib.ConsumerRtBuffer(sample_period=sample_period,
                                                                  max_samples=self._max_realtime_samples)
                        self.rt_buffers[d_handle] = rt_buffer
                    # last_sc = rt_buffer.last_sc
                    rt_sample_containers = rt_buffer.mk_rt_sample_containers(new_sac)
                    rt_buffer.add_rt_sample_containers(rt_sample_containers)

                    # check age
                    if len(rt_sample_containers) > 0:
                        waveform_age[d_handle] = rt_sample_containers[-1].age

                    # check descriptor version
                    if descriptor_container.DescriptorVersion != new_sac.DescriptorVersion:
                        self._logger.error('_onWaveformReport: descriptor {}: expect version "{}", found "{}"',
                                           d_handle, new_sac.DescriptorVersion, descriptor_container.DescriptorVersion)

            # if len(waveform_age) > 0:
            #     min_age = min(waveform_age.values())
            #     max_age = max(waveform_age.values())
            #     shall_log = self.waveform_time_warner.getOutOfDeterminationTimeLogState(
            #         min_age, max_age, self.DETERMINATIONTIME_WARN_LIMIT)
            #     if shall_log != A_NO_LOG:
            #         tmp = ', '.join('"{}":{:.3f}sec.'.format(k, v) for k, v in waveform_age.items())
            #         if shall_log == A_OUT_OF_RANGE:
            #             self._logger.warn(
            #                 '_onWaveformReport mdibVersion {}: age of samples outside limit of {} sec.: age={}!',
            #                 new_mdib_version, self.DETERMINATIONTIME_WARN_LIMIT, tmp)
            #         elif shall_log == A_STILL_OUT_OF_RANGE:
            #             self._logger.warn(
            #                 '_onWaveformReport mdibVersion {}: age of samples still outside limit of {} sec.: age={}!',
            #                 new_mdib_version, self.DETERMINATIONTIME_WARN_LIMIT, tmp)
            #         elif shall_log == A_BACK_IN_RANGE:
            #             self._logger.info(
            #                 '_onWaveformReport mdibVersion {}: age of samples back in limit of {} sec.: age={}',
            #                 new_mdib_version, self.DETERMINATIONTIME_WARN_LIMIT, tmp)
            # if LOG_WF_AGE_INTERVAL:
            #     now = time.time()
            #     if now - self._last_wf_age_log >= LOG_WF_AGE_INTERVAL:
            #         age_data = self.get_wf_age_stdev()
            #         self._logger.info('waveform mean age={:.1f}ms., std-dev={:.2f}ms. min={:.1f}ms., max={}',
            #                           age_data.mean_age * 1000., age_data.stdev * 1000.,
            #                           age_data.min_age * 1000., age_data.max_age * 1000.)
            #         self._last_wf_age_log = now
        finally:
            self.waveform_by_handle = waveform_by_handle

    def _on_episodic_component_report(self, report_data: EpisodicReportData, is_buffered_report):
        report = report_data.p_response.report
        component_by_handle = {}
        state_containers = []
        for report_part in report.component.abstract_component_report.report_part:
            state_containers.extend(report_data.msg_reader.read_states(report_part.component_state, self))
        try:
            with self.mdib_lock:
                self.mdib_version = report_data.mdib_version_group.mdib_version
                if report_data.mdib_version_group.sequence_id != self.sequence_id:
                    self.sequence_id = report_data.mdib_version_group.sequence_id
                for sc in state_containers:
                    desc_h = sc.DescriptorHandle
                    try:
                        old_state_container = self.states.descriptor_handle.get_one(desc_h, allow_none=True)
                    except RuntimeError as ex:
                        self._logger.error('_on_episodic_component_report, get_one on states: {}', ex)
                        continue

                    if old_state_container is None:
                        self.states.add_object(sc)
                        self._logger.info(
                            '_onEpisodicComponentReport: new component state handle = {} DescriptorVersion={}',
                            desc_h, sc.DescriptorVersion)
                        component_by_handle[sc.descriptorHandle] = sc
                    else:
                        if self._has_new_state_usable_state_version(old_state_container, sc, 'EpisodicComponentReport',
                                                                    is_buffered_report):
                            self._logger.info(
                                '_onEpisodicComponentReport: updated component state, handle="{}" DescriptorVersion={}',
                                desc_h, sc.DescriptorVersion)
                            old_state_container.update_from_other_container(sc)
                            self.states.update_object(old_state_container)
                            component_by_handle[old_state_container.DescriptorHandle] = old_state_container
        finally:
            self.component_by_handle = component_by_handle

    def _on_episodic_operational_state_report(self, report_data: EpisodicReportData, is_buffered_report):
        report = report_data.p_response.report
        operation_by_handle = {}
        state_containers = []
        for report_part in report.operational_state.abstract_operational_state_report.report_part:
            state_containers.extend(report_data.msg_reader.read_states(report_part.operation_state, self))
        try:
            with self.mdib_lock:
                self.mdib_version = report_data.mdib_version_group.mdib_version
                if report_data.mdib_version_group.sequence_id != self.sequence_id:
                    self.sequence_id = report_data.mdib_version_group.sequence_id
                for sc in state_containers:
                    desc_h = sc.DescriptorHandle
                    try:
                        old_state_container = self.states.descriptor_handle.get_one(desc_h, allow_none=True)
                    except RuntimeError as ex:
                        self._logger.error('_on_episodic_operational_state_report, get_one on states: {}', ex)
                        continue

                    if old_state_container is None:
                        self.states.add_object(sc)
                        self._logger.info(
                            '_on_episodic_operational_state_report: new operational state handle = {} DescriptorVersion={}',
                            desc_h, sc.DescriptorVersion)
                        operation_by_handle[sc.descriptorHandle] = sc
                    else:
                        if self._has_new_state_usable_state_version(old_state_container, sc,
                                                                    'EpisodicOperationalStateReport',
                                                                    is_buffered_report):
                            self._logger.info(
                                '_on_episodic_operational_state_report: updated component state, handle="{}" DescriptorVersion={}',
                                desc_h, sc.DescriptorVersion)
                            old_state_container.update_from_other_container(sc)
                            self.states.update_object(old_state_container)
                            operation_by_handle[old_state_container.DescriptorHandle] = old_state_container
        finally:
            self.operation_by_handle = operation_by_handle

    def _on_episodic_context_report(self, report_data: EpisodicReportData, is_buffered_report):
        report = report_data.p_response.report
        context_by_handle = {}
        state_containers = []
        for report_part in report.context.abstract_context_report.report_part:
            state_containers.extend(report_data.msg_reader.read_states(report_part.context_state, self))
        try:
            with self.mdib_lock:
                self.mdib_version = report_data.mdib_version_group.mdib_version
                if report_data.mdib_version_group.sequence_id != self.sequence_id:
                    self.sequence_id = report_data.mdib_version_group.sequence_id
                for sc in state_containers:
                    try:
                        old_state_container = self.context_states.handle.get_one(sc.Handle, allow_none=True)
                    except RuntimeError as ex:
                        self._logger.error('_onEpisodicContextReport, get_one on contextStates: {}', ex)
                        continue

                    if old_state_container is None:
                        self.context_states.add_object(sc)
                        self._logger.info(
                            '_on_episodic_context_report: new context state handle = {} Descriptor Handle={} Assoc={}, Validators={}',
                            sc.Handle, sc.DescriptorHandle, sc.ContextAssociation, sc.Validator)
                        context_by_handle[sc.Handle] = sc
                    else:
                        if self._has_new_state_usable_state_version(old_state_container, sc, 'EpisodicContextReport',
                                                                    is_buffered_report):
                            self._logger.info(
                                '_on_episodic_context_report: updated context state handle = {} Descriptor Handle={} Assoc={}, Validators={}',
                                sc.Handle, sc.DescriptorHandle, sc.ContextAssociation, sc.Validator)
                            old_state_container.update_from_other_container(sc)
                            self.context_states.update_object(old_state_container)
                            context_by_handle[old_state_container.Handle] = old_state_container
        finally:
            self.context_by_handle = context_by_handle

    def _on_description_modification_report(self, report_data: EpisodicReportData, is_buffered_report):
        report = report_data.p_response.report

        new_descriptor_by_handle = {}
        updated_descriptor_by_handle = {}
        deleted_descriptor_by_handle = {}
        with self.mdib_lock:
            self.mdib_version = report_data.mdib_version_group.mdib_version
            if report_data.mdib_version_group.sequence_id != self.sequence_id:
                self.sequence_id = report_data.mdib_version_group.sequence_id

            for report_part in report.description.report_part:
                descriptor_containers = report_data.msg_reader.read_descriptors(
                    report_part.p_descriptor,
                    get_p_attr(report_part,'ParentDescriptor').string)
                modification_type = enum_attr_from_p(report_part,
                                                     'ModificationType',
                                                     msg_types.DescriptionModificationType)
                if modification_type == msg_types.DescriptionModificationType.CREATE:  # CRT
                    for dc in descriptor_containers:
                        self.descriptions.add_object(dc)
                        self._logger.debug('_onDescriptionModificationReport: created description "{}" (parent="{}")',
                                           dc.Handle, dc.parent_handle)
                        new_descriptor_by_handle[dc.Handle] = dc
                    state_containers = report_data.msg_reader.read_states(report_part.state, self)
                    for sc in state_containers:
                        # determine multikey
                        if sc.is_context_state:
                            multikey = self.context_states
                        else:
                            multikey = self.states
                        multikey.add_object(sc)
                elif modification_type == msg_types.DescriptionModificationType.UPDATE:  # UPD
                    for dc in descriptor_containers:
                        self._logger.info('_onDescriptionModificationReport: update descriptor "{}" (parent="{}")',
                                          dc.Handle, dc.parent_handle)
                        container = self.descriptions.handle.get_one(dc.Handle, allow_none=True)
                        if container is None:
                            pass
                        else:
                            container.update_from_other_container(dc)
                        updated_descriptor_by_handle[dc.Handle] = dc
                    state_containers = report_data.msg_reader.read_states(report_part.state, self)
                    for sc in state_containers:
                        # determine multikey
                        if sc.is_context_state:
                            multikey = self.context_states
                            old_state_container = multikey.handle.get_one(sc.Handle, allow_none=True)
                        else:
                            multikey = self.states
                            old_state_container = multikey.descriptor_handle.get_one(sc.DescriptorHandle,
                                                                                     allow_none=True)
                        if old_state_container is not None:
                            old_state_container.update_from_other_container(sc)
                            multikey.update_object(old_state_container)
                else:  # DEL
                    for dc in descriptor_containers:
                        self._logger.debug('_onDescriptionModificationReport: remove descriptor "{}" (parent="{}")',
                                           dc.Handle, dc.parent_handle)
                        self.rm_descriptor_by_handle(
                            dc.Handle)  # handling of self.deletedDescriptorByHandle inside called method
                        deleted_descriptor_by_handle[dc.Handle] = dc

                self.description_modifications = report
                # write observables for every report part separately
                if new_descriptor_by_handle:
                    self.new_descriptors_by_handle = new_descriptor_by_handle
                if updated_descriptor_by_handle:
                    self.updated_descriptors_by_handle = updated_descriptor_by_handle
                if deleted_descriptor_by_handle:
                    self.deleted_descriptors_by_handle = deleted_descriptor_by_handle
