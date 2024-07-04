import unittest
from decimal import Decimal
from lxml import etree
from sdc11073.xml_types import pm_types
from sdc11073.xml_types import msg_qnames
from sdc11073.namespaces import default_ns_helper as nsh

from sdc11073.mdib import descriptorcontainers as dc
from sdc11073.loghelper import basic_logging_setup
from pyprotosdc.mapping import descriptorsmapper as dm
from pyprotosdc.mapping.extension_mapping import extension_to_p

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

    def test_mds_descriptor(self):
        mds_max = dc.MdsDescriptorContainer('my_handle', 'p_handle')
        mds_max.ProductionSpecification = [pm_types.ProductionSpecification(pm_types.CodedValue('abc', 'def'), 'prod_spec')]
        mds_max.Manufacturer = [pm_types.LocalizedText('some_company')]
        mds_min = dc.MdsDescriptorContainer('my_handle', None)
        for obj in (mds_max, mds_min):
            self.check_convert(obj)

    def test_vmd_descriptor(self):
        obj = dc.VmdDescriptorContainer('my_handle', 'p_handle')
        self.check_convert(obj)

    def test_channel_descriptor(self):
        obj = dc.ChannelDescriptorContainer('my_handle', None)
        self.check_convert(obj)

    def test_clock_descriptor(self):
        descr_max = dc.ClockDescriptorContainer('my_handle', 'p_handle')
        descr_max.TimeProtocol = [pm_types.CodedValue('abc', 'def'), pm_types.CodedValue('123', '456')]
        descr_max.Resolution = 42.1
        descr_min = dc.ClockDescriptorContainer('my_handle', None)
        for obj in (descr_max, descr_min):
            self.check_convert(obj)

    def test_battery_descriptor(self):
        descr_max = dc.BatteryDescriptorContainer('my_handle', 'p_handle')
        descr_max.CapacityFullCharge = pm_types.Measurement(Decimal('11'), pm_types.CodedValue('ah'))
        descr_max.CapacitySpecified = pm_types.Measurement(Decimal('12'), pm_types.CodedValue('ah'))
        descr_max.VoltageSpecified = pm_types.Measurement(Decimal('6'), pm_types.CodedValue('v'))
        descr_min = dc.BatteryDescriptorContainer('my_handle', None)
        for obj in (descr_max, descr_min):
            self.check_convert(obj)

    def test_sco_descriptor(self):
        obj = dc.ScoDescriptorContainer('my_handle', 'p_handle')
        self.check_convert(obj)

    def test_numeric_metric_descriptor(self):
        # properties of AbstractMetricDescriptorContainer
        descr_max = dc.NumericMetricDescriptorContainer('my_handle', 'p_handle')
        descr_max.Unit = pm_types.CodedValue('ah')
        descr_max.BodySite = [pm_types.CodedValue('ah')]
        descr_max.Relation = [pm_types.Relation()]
        descr_max.MetricCategory = pm_types.MetricCategory.PRESETTING #'Preset'
        descr_max.DerivationMethod = pm_types.DerivationMethod.MANUAL #'Man'
        descr_max.MetricAvailability = pm_types.MetricAvailability.INTERMITTENT
        descr_max.MaxMeasurementTime = 3
        descr_max.MaxDelayTime = 1
        descr_max.DeterminationPeriod = 1
        descr_max.LifeTimePeriod = 2
        descr_max.ActivationDuration = 3
        # properties of NumericMetricDescriptorContainer
        descr_max.TechnicalRange = [pm_types.Range(Decimal(0), Decimal(20), Decimal(0.5))]
        descr_max.Resolution = Decimal('0.01')
        descr_max.AveragingPeriod = 0.02

        descr_min = dc.NumericMetricDescriptorContainer('my_handle', None)
        descr_min.Resolution = Decimal('0.02')
        for obj in (descr_max, descr_min):
            self.check_convert(obj)


    def test_string_metric_descriptor(self):
        # properties of AbstractMetricDescriptorContainer
        descr_max = dc.StringMetricDescriptorContainer('my_handle', 'p_handle')
        descr_max.Unit = pm_types.CodedValue('ah')
        descr_max.BodySite = [pm_types.CodedValue('ah')]
        descr_max.Relation = [pm_types.Relation()]
        descr_max.MetricCategory = pm_types.MetricCategory.PRESETTING #'Preset'
        descr_max.DerivationMethod = pm_types.DerivationMethod.MANUAL #'Man'
        descr_max.MetricAvailability = pm_types.MetricAvailability.INTERMITTENT
        descr_max.MaxMeasurementTime = 3
        descr_max.MaxDelayTime = 1
        descr_max.DeterminationPeriod = 1
        descr_max.LifeTimePeriod = 2
        descr_max.ActivationDuration = 3

        descr_min = dc.StringMetricDescriptorContainer('my_handle', None)
        descr_min.Resolution = Decimal('0.02')
        for obj in (descr_max, descr_min):
            self.check_convert(obj)

    def test_enum_string_metric_descriptor(self):
        # properties of AbstractMetricDescriptorContainer
        descr_max = dc.EnumStringMetricDescriptorContainer('my_handle', 'p_handle')
        descr_max.Unit = pm_types.CodedValue('ah')
        descr_max.BodySite = [pm_types.CodedValue('ah')]
        descr_max.Relation = [pm_types.Relation()]
        descr_max.MetricCategory = pm_types.MetricCategory.PRESETTING #'Preset'
        descr_max.DerivationMethod = pm_types.DerivationMethod.MANUAL #'Man'
        descr_max.MetricAvailability = pm_types.MetricAvailability.INTERMITTENT
        descr_max.MaxMeasurementTime = 3
        descr_max.MaxDelayTime = 1
        descr_max.DeterminationPeriod = 1
        descr_max.LifeTimePeriod = 2
        descr_max.ActivationDuration = 3
        descr_max.AllowedValue = [pm_types.AllowedValue('an_allowed_value', pm_types.CodedValue('abc')),
                                  pm_types.AllowedValue('another_allowed_value', pm_types.CodedValue('def'))]

        descr_min = dc.StringMetricDescriptorContainer('my_handle', None)
        descr_min.Resolution = Decimal('0.02')
        for obj in (descr_max, descr_min):
            self.check_convert(obj)

    def test_realtime_sample_array_metric_descriptor(self):
        # properties of AbstractMetricDescriptorContainer
        descr_max = dc.RealTimeSampleArrayMetricDescriptorContainer('my_handle', 'p_handle')
        descr_max.Unit = pm_types.CodedValue('ah')
        descr_max.BodySite = [pm_types.CodedValue('ah')]
        descr_max.Relation = [pm_types.Relation()]
        descr_max.MetricCategory = pm_types.MetricCategory.PRESETTING #'Preset'
        descr_max.DerivationMethod = pm_types.DerivationMethod.MANUAL #'Man'
        descr_max.MetricAvailability = pm_types.MetricAvailability.INTERMITTENT
        descr_max.MaxMeasurementTime = 3
        descr_max.MaxDelayTime = 1
        descr_max.DeterminationPeriod = 1
        descr_max.LifeTimePeriod = 2
        descr_max.ActivationDuration = 3

        descr_max.TechnicalRange = [pm_types.Range(Decimal(0), Decimal(1))]
        descr_max.Resolution = Decimal('0.02')
        descr_max.SamplePeriod = 0.005
        descr_min = dc.RealTimeSampleArrayMetricDescriptorContainer('my_handle', None)
        descr_min.Resolution = Decimal('0.02')
        descr_min.SamplePeriod = 0.005
        for obj in (descr_max, descr_min):
            self.check_convert(obj)

    def test_distribution_sample_array_metric_descriptor(self):
        # properties of AbstractMetricDescriptorContainer
        descr_max = dc.DistributionSampleArrayMetricDescriptorContainer('my_handle', 'p_handle')
        descr_max.Unit = pm_types.CodedValue('ah')
        descr_max.BodySite = [pm_types.CodedValue('ah')]
        descr_max.Relation = [pm_types.Relation()]
        descr_max.MetricCategory = pm_types.MetricCategory.PRESETTING #'Preset'
        descr_max.DerivationMethod = pm_types.DerivationMethod.MANUAL #'Man'
        descr_max.MetricAvailability = pm_types.MetricAvailability.INTERMITTENT
        descr_max.MaxMeasurementTime = 3
        descr_max.MaxDelayTime = 1
        descr_max.DeterminationPeriod = 1
        descr_max.LifeTimePeriod = 2
        descr_max.ActivationDuration = 3

        descr_max.TechnicalRange = [pm_types.Range(Decimal(0), Decimal(1))]
        descr_max.Resolution = Decimal('0.02')
        descr_max.DomainUnit = pm_types.CodedValue('abc')
        descr_max.DistributionRange = pm_types.Range(Decimal(0), Decimal(200), Decimal(0), Decimal(1))
        descr_min = dc.DistributionSampleArrayMetricDescriptorContainer('my_handle', None)
        descr_min.Resolution = Decimal('0.02')
        for obj in (descr_max, descr_min):
            self.check_convert(obj)

    def _set_abstract_operation_descriptor(self, descr, max):
        descr.OperationTarget = '4711'
        if not max:
            return
        descr.MaxTimeToFinish = 1.1
        descr.InvocationEffectiveTimeout = 2
        descr.Retriggerable = False
        descr.MaxLength = 12

    def _set_abstract_set_state_operation_descriptor(self, descr, max):
        self._set_abstract_operation_descriptor(descr, max)
        if not max:
            return
        descr.ModifiableData = ['a', 'b']

    def test_set_value_operation_descriptor(self):
        descr_max = dc.SetValueOperationDescriptorContainer('my_handle', 'p_handle')
        self._set_abstract_operation_descriptor(descr_max, True)
        descr_min = dc.SetValueOperationDescriptorContainer('my_handle', None)
        self._set_abstract_operation_descriptor(descr_min, False)
        for obj in (descr_max, descr_min):
            self.check_convert(obj)

    def test_set_string_operation_descriptor(self):
        descr_max = dc.SetStringOperationDescriptorContainer('my_handle', 'p_handle')
        self._set_abstract_operation_descriptor(descr_max, True)
        descr_min = dc.SetStringOperationDescriptorContainer('my_handle', None)
        self._set_abstract_operation_descriptor(descr_min, False)
        for obj in (descr_max, descr_min):
            self.check_convert(obj)

    def test_set_context_state_operation_descriptor(self):
        descr_max = dc.SetContextStateOperationDescriptorContainer('my_handle', 'p_handle')
        self._set_abstract_set_state_operation_descriptor(descr_max, True)
        descr_min = dc.SetContextStateOperationDescriptorContainer('my_handle', None)
        self._set_abstract_set_state_operation_descriptor(descr_min, True)
        for obj in (descr_max, descr_min):
            self.check_convert(obj)

    def test_set_metric_state_operation_descriptor(self):
        descr_max = dc.SetMetricStateOperationDescriptorContainer('my_handle', 'p_handle')
        self._set_abstract_set_state_operation_descriptor(descr_max, True)
        descr_min = dc.SetMetricStateOperationDescriptorContainer('my_handle', None)
        self._set_abstract_set_state_operation_descriptor(descr_min, True)
        for obj in (descr_max, descr_min):
            self.check_convert(obj)

    def test_set_component_state_operation_descriptor(self):
        descr_max = dc.SetComponentStateOperationDescriptorContainer('my_handle', 'p_handle')
        self._set_abstract_set_state_operation_descriptor(descr_max, True)
        descr_min = dc.SetComponentStateOperationDescriptorContainer('my_handle', None)
        self._set_abstract_set_state_operation_descriptor(descr_min, True)
        for obj in (descr_max, descr_min):
            self.check_convert(obj)

    def test_set_alert_state_operation_descriptor(self):
        descr_max = dc.SetAlertStateOperationDescriptorContainer('my_handle', 'p_handle')
        self._set_abstract_set_state_operation_descriptor(descr_max, True)
        descr_min = dc.SetAlertStateOperationDescriptorContainer('my_handle', None)
        self._set_abstract_set_state_operation_descriptor(descr_min, True)
        for obj in (descr_max, descr_min):
            self.check_convert(obj)

    def test_activate_operation_descriptor(self):
        descr_max = dc.ActivateOperationDescriptorContainer('my_handle', 'p_handle')
        self._set_abstract_set_state_operation_descriptor(descr_max, True)
        descr_max.Argument = [pm_types.ActivateOperationDescriptorArgument(pm_types.CodedValue('aaa'), nsh.PM.tag('oh')),
                              pm_types.ActivateOperationDescriptorArgument(pm_types.CodedValue('bbb'), nsh.PM.tag('nooo')), ]
        descr_min = dc.ActivateOperationDescriptorContainer('my_handle', None)
        self._set_abstract_set_state_operation_descriptor(descr_min, True)
        for obj in (descr_max, descr_min):
            self.check_convert(obj)

    def test_alert_system_descriptor(self):
        descr_max = dc.AlertSystemDescriptorContainer('my_handle', 'p_handle')
        descr_max.MaxPhysiologicalParallelAlarms = 3
        descr_max.MaxTechnicalParallelAlarms = 2
        descr_max.SelfCheckPeriod = 60.0
        descr_max.Argument = [pm_types.ActivateOperationDescriptorArgument(pm_types.CodedValue('aaa'), nsh.PM.tag('oh')),
                              pm_types.ActivateOperationDescriptorArgument(pm_types.CodedValue('bbb'), nsh.PM.tag('nooo')), ]
        descr_min = dc.AlertSystemDescriptorContainer('my_handle', None)
        for obj in (descr_max, descr_min):
            self.check_convert(obj)

    def test_alert_condition_descriptor(self):
        descr_max = dc.AlertConditionDescriptorContainer('my_handle', 'p_handle')
        descr_max.Source = ['a', 'b']
        descr_max.CauseInfo = [pm_types.CauseInfo(pm_types.RemedyInfo([pm_types.LocalizedText('rembla')]),
                                                  [pm_types.LocalizedText('caubla')])]
        descr_max.Kind = pm_types.AlertConditionKind.PHYSIOLOGICAL
        descr_max.Priority = pm_types.AlertConditionPriority.MEDIUM
        descr_max.DefaultConditionGenerationDelay = 2.5
        descr_max.CanEscalate = pm_types.CanEscalate.MEDIUM
        descr_max.CanDeescalate = pm_types.CanDeEscalate.LOW
        descr_min = dc.AlertConditionDescriptorContainer('my_handle', None)
        for obj in (descr_max, descr_min):
            self.check_convert(obj)

    def test_limit_alert_condition_descriptor(self):
        descr_max = dc.LimitAlertConditionDescriptorContainer('my_handle', 'p_handle')
        descr_max.Source = ['a', 'b']
        descr_max.CauseInfo = [pm_types.CauseInfo(pm_types.RemedyInfo([pm_types.LocalizedText('rembla')]),
                                                  [pm_types.LocalizedText('caubla')])]
        descr_max.Kind = pm_types.AlertConditionKind.PHYSIOLOGICAL
        descr_max.Priority = pm_types.AlertConditionPriority.MEDIUM
        descr_max.DefaultConditionGenerationDelay = 2.5
        descr_max.CanEscalate = pm_types.CanEscalate.MEDIUM
        descr_max.CanDeescalate = pm_types.CanDeEscalate.LOW
        descr_max.MaxLimits = pm_types.Range(Decimal(0), Decimal(10))
        descr_max.AutoLimitSupported = True
        descr_min = dc.LimitAlertConditionDescriptorContainer('my_handle', None)
        for obj in (descr_max, descr_min):
            self.check_convert(obj)

    def test_alert_signal_descriptor(self):
        descr_max = dc.AlertSignalDescriptorContainer('my_handle', 'p_handle')
        descr_max.ConditionSignaled = 'handle123'
        descr_max.Manifestation = pm_types.AlertSignalManifestation.AUD
        descr_max.Latching = True
        descr_max.DefaultSignalGenerationDelay = 1.5
        descr_max.SignalDelegationSupported = True
        descr_max.AcknowledgementSupported = True
        descr_max.AcknowledgeTimeout = 5.5
        descr_min = dc.AlertSignalDescriptorContainer('my_handle', None)
        for obj in (descr_max, descr_min):
            self.check_convert(obj)

    def test_context_descriptors(self):
        # all these classes have no own properties, therefore no need for max and min variants
        for descr_cls in (dc.PatientContextDescriptorContainer,
                          dc.LocationContextDescriptorContainer,
                          dc.WorkflowContextDescriptorContainer,
                          dc.OperatorContextDescriptorContainer,
                          dc.MeansContextDescriptorContainer,
                          dc.EnsembleContextDescriptorContainer):
            descr = descr_cls('my_handle', 'p_handle')
            self.check_convert(descr)
