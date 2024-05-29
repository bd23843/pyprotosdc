import unittest
from decimal import Decimal
import time
from sdc11073.xml_types import pm_types as pmtypes

from sdc11073.mdib import statecontainers as sc
from sdc11073.mdib.descriptorcontainers import AbstractDescriptorContainer
from sdc11073.loghelper import basic_logging_setup
from pyprotosdc.mapping import statesmapper as sm

class TestStateMappers(unittest.TestCase):
    def setUp(self) -> None:
        basic_logging_setup()
        self.descr = AbstractDescriptorContainer('my_handle', 'parent_handle')
        self.descr.DescriptorVersion = 42

    def tearDown(self) -> None:
        pass

    def check_convert(self,obj):
        obj_p = sm.generic_state_to_p(obj, None)
        obj2 = sm.generic_state_from_p(obj_p, self.descr)
        self.assertEqual(obj.__class__, obj2.__class__)
        delta = obj.diff(obj2)
        if delta:
            print(f'delta = {delta}')

        self.assertIsNone(obj.diff(obj2))
        self.assertEqual(obj.DescriptorVersion, self.descr.DescriptorVersion)

    def test_set_value_operation_state(self):
        st_max = sc.SetValueOperationStateContainer(self.descr)
        st_max.AllowedRange = [pmtypes.Range(Decimal(0), Decimal(100.0))]
        st_min = sc.SetValueOperationStateContainer(self.descr)
        for obj in (st_max, st_min):
            self.check_convert(obj)

    def test_set_string_operation_state(self):
        st_max = sc.SetStringOperationStateContainer(self.descr)
        st_max.AllowedValues.Value.extend( ['a', 'b'])
        st_min = sc.SetStringOperationStateContainer(self.descr)
        for obj in (st_max, st_min):
            self.check_convert(obj)

    def test_operation_states(self):
        for state_cls in [sc.ActivateOperationStateContainer,
                           sc.SetContextStateOperationStateContainer,
                           sc.SetMetricStateOperationStateContainer,
                           sc.SetComponentStateOperationStateContainer,
                           sc.SetAlertStateOperationStateContainer]:
            obj = state_cls(self.descr)
            self.check_convert(obj)

    def test_numeric_metric_state(self):
        st_max = sc.NumericMetricStateContainer(self.descr)
        st_max.mk_metric_value()
        st_max.BodySite = [pmtypes.CodedValue('abc'), pmtypes.CodedValue('def')]
        st_max.PhysicalConnector = pmtypes.PhysicalConnectorInfo(
            [pmtypes.LocalizedText('foo'), pmtypes.LocalizedText('bar')], 42)
        st_max.ActivationState = pmtypes.ComponentActivation.NOT_READY
        st_max.ActiveDeterminationPeriod = 5.5
        st_max.LifeTimePeriod = 9.0
        st_min = sc.NumericMetricStateContainer(self.descr)
        for obj in (st_max, st_min):
            self.check_convert(obj)

    def test_string_metric_state(self):
        st_max = sc.StringMetricStateContainer(self.descr)
        st_max.mk_metric_value()
        st_min = sc.StringMetricStateContainer(self.descr)
        for obj in (st_max, st_min):
            self.check_convert(obj)

    def test_enum_string_metric_state(self):
        st_max = sc.EnumStringMetricStateContainer(self.descr)
        st_max.mk_metric_value()
        st_min = sc.EnumStringMetricStateContainer(self.descr)
        for obj in (st_max, st_min):
            self.check_convert(obj)

    def test_realtime_sample_array_metric_state(self):
        st_max = sc.RealTimeSampleArrayMetricStateContainer(self.descr)
        st_max.mk_metric_value()
        st_max.PhysiologicalRange = [pmtypes.Range(Decimal(1), Decimal(2)), pmtypes.Range(Decimal(3), Decimal(4))]
        st_min = sc.RealTimeSampleArrayMetricStateContainer(self.descr)
        for obj in (st_max, st_min):
            self.check_convert(obj)

    def test_distribution_sample_array_metric_state(self):
        st_max = sc.DistributionSampleArrayMetricStateContainer(self.descr)
        st_max.mk_metric_value()
        st_max.PhysiologicalRange = [pmtypes.Range(Decimal(1), Decimal(2)), pmtypes.Range(Decimal(3), Decimal(4))]
        st_min = sc.DistributionSampleArrayMetricStateContainer(self.descr)
        for obj in (st_max, st_min):
            self.check_convert(obj)

    def test_mds_state(self):
        st_max = sc.MdsStateContainer(self.descr)
        # AbstractStateContainer
        st_max.StateVersion = 5
        # AbstractDeviceComponentStateContainer
        # st_max.CalibrationInfo = cp.NotImplementedProperty('CalibrationInfo', None)  # optional, CalibrationInfo type
        # st_max.NextCalibration = cp.NotImplementedProperty('NextCalibration', None)  # optional, CalibrationInfo type
        st_max.PhysicalConnector = pmtypes.PhysicalConnectorInfo(
            [pmtypes.LocalizedText('foo'), pmtypes.LocalizedText('bar')], 42)

        st_max.ActivationState = pmtypes.ComponentActivation.FAILURE
        st_max.OperatingHours = 42
        st_max.OperatingCycles = 101
        # MdsStateContainer
        st_max.OperatingMode = pmtypes.MdsOperatingMode.DEMO
        st_max.Lang = 'xx'
        st_min = sc.MdsStateContainer(self.descr)
        for obj in (st_max, st_min):
            self.check_convert(obj)

    def test_sco_state(self):
        st_max = sc.ScoStateContainer(self.descr)
        # AbstractStateContainer
        st_max.StateVersion = 5
        # AbstractDeviceComponentStateContainer
        st_max.PhysicalConnector = pmtypes.PhysicalConnectorInfo(
            [pmtypes.LocalizedText('foo'), pmtypes.LocalizedText('bar')], 42)

        st_max.ActivationState = pmtypes.ComponentActivation.FAILURE
        st_max.OperatingHours = 42
        st_max.OperatingCycles = 101
        # ScoStateContainer
        st_max.OperationGroup = [pmtypes.OperationGroup(pmtypes.CodedValue('abc'))]
        st_max.InvocationRequested = ['handle1', 'handle2']
        st_max.InvocationRequired =  ['handle3', 'handle3']
        st_min = sc.ScoStateContainer(self.descr)
        for obj in (st_max, st_min):
            self.check_convert(obj)

    def test_vmd_state(self):
        st_max = sc.VmdStateContainer(self.descr)
        # AbstractStateContainer
        st_max.StateVersion = 5
        # AbstractDeviceComponentStateContainer
        st_max.PhysicalConnector = pmtypes.PhysicalConnectorInfo(
            [pmtypes.LocalizedText('foo'), pmtypes.LocalizedText('bar')], 42)

        st_max.ActivationState = pmtypes.ComponentActivation.FAILURE
        st_max.OperatingHours = 42
        st_max.OperatingCycles = 101
        st_min = sc.VmdStateContainer(self.descr)
        for obj in (st_max, st_min):
            self.check_convert(obj)

    def test_channel_state(self):
        st_max = sc.ChannelStateContainer(self.descr)
        # AbstractStateContainer
        st_max.StateVersion = 5
        # AbstractDeviceComponentStateContainer
        st_max.PhysicalConnector = pmtypes.PhysicalConnectorInfo(
            [pmtypes.LocalizedText('foo'), pmtypes.LocalizedText('bar')], 42)

        st_max.ActivationState = pmtypes.ComponentActivation.FAILURE
        st_max.OperatingHours = 42
        st_max.OperatingCycles = 101
        st_min = sc.ChannelStateContainer(self.descr)
        for obj in (st_max, st_min):
            self.check_convert(obj)

    def test_clock_state(self):
        st_max = sc.ClockStateContainer(self.descr)
        # AbstractStateContainer
        st_max.StateVersion = 5
        # AbstractDeviceComponentStateContainer
        st_max.PhysicalConnector = pmtypes.PhysicalConnectorInfo(
            [pmtypes.LocalizedText('foo'), pmtypes.LocalizedText('bar')], 42)

        st_max.ActivationState = pmtypes.ComponentActivation.FAILURE
        st_max.OperatingHours = 42
        st_max.OperatingCycles = 101
        st_min = sc.ClockStateContainer(self.descr)
        for obj in (st_max, st_min):
            self.check_convert(obj)

    def test_system_context_state(self):
        st_max = sc.SystemContextStateContainer(self.descr)
        # AbstractStateContainer
        st_max.StateVersion = 5
        # AbstractDeviceComponentStateContainer
        st_max.PhysicalConnector = pmtypes.PhysicalConnectorInfo(
            [pmtypes.LocalizedText('foo'), pmtypes.LocalizedText('bar')], 42)

        st_max.ActivationState = pmtypes.ComponentActivation.FAILURE
        st_max.OperatingHours = 42
        st_max.OperatingCycles = 101
        st_min = sc.SystemContextStateContainer(self.descr)
        for obj in (st_max, st_min):
            self.check_convert(obj)

    def test_battery_state(self):
        st_max = sc.BatteryStateContainer(self.descr)
        # AbstractStateContainer
        st_max.StateVersion = 5
        # AbstractDeviceComponentStateContainer
        st_max.PhysicalConnector = pmtypes.PhysicalConnectorInfo(
            [pmtypes.LocalizedText('foo'), pmtypes.LocalizedText('bar')], 42)

        st_max.ActivationState = pmtypes.ComponentActivation.FAILURE
        st_max.OperatingHours = 42
        st_max.OperatingCycles = 101
        # BatteryState
        st_max.CapacityRemaining = pmtypes.Measurement(Decimal(42), pmtypes.CodedValue('abc'))
        st_max.Voltage = pmtypes.Measurement(Decimal(12), pmtypes.CodedValue('def'))
        st_max.Current = pmtypes.Measurement(Decimal(2), pmtypes.CodedValue('xyz'))
        st_max.Temperature = pmtypes.Measurement(Decimal(70), pmtypes.CodedValue('xyz'))
        st_max.RemainingBatteryTime = pmtypes.Measurement(Decimal(3), pmtypes.CodedValue('xyz'))
        st_max.ChargeStatus = sc.BatteryStateContainer.ChargeStatusEnum.CHARGING
        st_max.ChargeCycles = 123
        st_min = sc.BatteryStateContainer(self.descr)
        for obj in (st_max, st_min):
            self.check_convert(obj)

    def test_alert_system_state(self):
        st_max = sc.AlertSystemStateContainer(self.descr)
        # AbstractState
        st_max.StateVersion = 5
        # AbstractAlertState
        st_max.ActivationState = pmtypes.AlertActivation.PAUSED
        # AlertSystemState
        st_max.SystemSignalActivation
        st_max.LastSelfCheck = int(time.time())
        st_max.SelfCheckCount = 3
        st_max.PresentPhysiologicalAlarmConditions = ['a', 'b']
        st_max.PresentTechnicalAlarmConditions = ['c', 'd']
        st_min = sc.AlertSystemStateContainer(self.descr)
        for obj in (st_max, st_min):
            self.check_convert(obj)

    def test_alert_signal_state(self):
        self.descr.SignalDelegationSupported = True
        st_max = sc.AlertSignalStateContainer(self.descr)
        # AbstractState
        st_max.StateVersion = 5
        # AbstractAlertState
        st_max.ActivationState = pmtypes.AlertActivation.PAUSED
        # AlertSignalState
        st_max.ActualSignalGenerationDelay = 0.2
        st_max.Presence = pmtypes.AlertSignalPresence.ACK
        st_max.Location = pmtypes.AlertSignalPrimaryLocation.REMOTE
        st_max.Slot = 3

        st_min = sc.AlertSystemStateContainer(self.descr)
        for obj in (st_max, st_min):
            self.check_convert(obj)

    def test_alert_condition_state(self):
        st_max = sc.AlertConditionStateContainer(self.descr)
        # AbstractState
        st_max.StateVersion = 5
        # AbstractAlertState
        st_max.ActivationState = pmtypes.AlertActivation.PAUSED
        # AlertConditionState
        st_max.ActualConditionGenerationDelay = 0.2
        st_max.ActualPriority = pmtypes.AlertConditionPriority.HIGH
        st_max.Rank = 2
        st_max.DeterminationTime = 1234567
        st_max.Presence = True

        st_min = sc.AlertConditionStateContainer(self.descr)
        for obj in (st_max, st_min):
            self.check_convert(obj)

    def test_limit_alert_condition_state(self):
        st_max = sc.LimitAlertConditionStateContainer(self.descr)
        # AbstractState
        st_max.StateVersion = 5
        # AbstractAlertState
        st_max.ActivationState = pmtypes.AlertActivation.PAUSED
        # AlertConditionState
        st_max.ActualConditionGenerationDelay = 0.2
        st_max.ActualPriority = pmtypes.AlertConditionPriority.HIGH
        st_max.Rank = 2
        st_max.DeterminationTime = 1234567
        st_max.Presence = True
        # LimitAlertConditionState
        st_max.Limits = pmtypes.Range(Decimal(0), Decimal(100), Decimal(0.5))
        st_max.MonitoredAlertLimits = pmtypes.AlertConditionMonitoredLimits.HIGH_OFF
        st_max.AutoLimitActivationState = pmtypes.AlertActivation.PAUSED
        st_min = sc.LimitAlertConditionStateContainer(self.descr)
        for obj in (st_max, st_min):
            self.check_convert(obj)

    def test_location_context_state(self):
        st_max = sc.LocationContextStateContainer(self.descr)
        st_max.Handle = 'abc'
        # AbstractState
        st_max.StateVersion = 5
        # AbstractContextState
        st_max.Validator = [pmtypes.InstanceIdentifier('abc')]
        st_max.Identification = [pmtypes.InstanceIdentifier('def')]
        st_max.ContextAssociation = pmtypes.ContextAssociation.PRE_ASSOCIATION
        st_max.BindingMdibVersion = 12
        st_max.UnbindingMdibVersion = 15
        st_max.BindingStartTime = 12345
        st_max.BindingEndTime = 12346
        # LocationContextState
        st_max.LocationDetail.Poc = 'Poc'
        st_max.LocationDetail.Room = 'Room'
        st_max.LocationDetail.Bed = 'Bed'
        st_max.LocationDetail.Facility = 'Facility'
        st_max.LocationDetail.Building = 'Building'
        st_max.LocationDetail.Floor = 'Floor'
        st_min = sc.LocationContextStateContainer(self.descr)
        st_min.Handle = 'def'
        for obj in (st_max, st_min):
            self.check_convert(obj)

    def test_patient_context_state(self):
        st_max = sc.PatientContextStateContainer(self.descr)
        st_max.Handle = 'abc'
        # AbstractState
        st_max.StateVersion = 5
        # AbstractContextState
        st_max.Validator = [pmtypes.InstanceIdentifier('abc')]
        st_max.Identification = [pmtypes.InstanceIdentifier('def')]
        st_max.ContextAssociation = pmtypes.ContextAssociation.PRE_ASSOCIATION
        st_max.BindingMdibVersion = 12
        st_max.UnbindingMdibVersion = 15
        st_max.BindingStartTime = 12345
        st_max.BindingEndTime = 12346
        st_max.CoreData.PoC = 'Poc'
        # PatientContextState
        st_max.CoreData.Givenname = 'gg'
        st_max.CoreData.Middlename = ['mm']
        st_max.CoreData.Familyname = 'ff'
        st_max.CoreData.Birthname = 'bb'
        st_max.CoreData.Title = 'tt'
        st_max.CoreData.Sex = pmtypes.SexType.FEMALE
        st_min = sc.PatientContextStateContainer(self.descr)
        st_min.Handle = 'def'
        for obj in (st_max, st_min):
            self.check_convert(obj)
