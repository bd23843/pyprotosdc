import inspect
import logging

from org.somda.protosdc.proto.model.biceps.abstractcomplexdevicecomponentstate_pb2 import \
    AbstractComplexDeviceComponentStateMsg
from org.somda.protosdc.proto.model.biceps.activateoperationstate_pb2 import ActivateOperationStateMsg
from org.somda.protosdc.proto.model.biceps.alertconditionstate_pb2 import AlertConditionStateMsg
from org.somda.protosdc.proto.model.biceps.alertsignalstate_pb2 import AlertSignalStateMsg
from org.somda.protosdc.proto.model.biceps.alertsystemstate_pb2 import AlertSystemStateMsg
from org.somda.protosdc.proto.model.biceps.batterystate_pb2 import BatteryStateMsg
from org.somda.protosdc.proto.model.biceps.channelstate_pb2 import ChannelStateMsg
from org.somda.protosdc.proto.model.biceps.clockstate_pb2 import ClockStateMsg
from org.somda.protosdc.proto.model.biceps.distributionsamplearraymetricstate_pb2 import \
    DistributionSampleArrayMetricStateMsg
from org.somda.protosdc.proto.model.biceps.enumstringmetricstate_pb2 import EnumStringMetricStateMsg
from org.somda.protosdc.proto.model.biceps.limitalertconditionstate_pb2 import LimitAlertConditionStateMsg
from org.somda.protosdc.proto.model.biceps.locationcontextstate_pb2 import LocationContextStateMsg
from org.somda.protosdc.proto.model.biceps.mdsstate_pb2 import MdsStateMsg
from org.somda.protosdc.proto.model.biceps.numericmetricstate_pb2 import NumericMetricStateMsg
from org.somda.protosdc.proto.model.biceps.patientcontextstate_pb2 import PatientContextStateMsg
from org.somda.protosdc.proto.model.biceps.realtimesamplearraymetricstate_pb2 import RealTimeSampleArrayMetricStateMsg
from org.somda.protosdc.proto.model.biceps.scostate_pb2 import ScoStateMsg
from org.somda.protosdc.proto.model.biceps.setalertstateoperationstate_pb2 import SetAlertStateOperationStateMsg
from org.somda.protosdc.proto.model.biceps.setcomponentstateoperationstate_pb2 import SetComponentStateOperationStateMsg
from org.somda.protosdc.proto.model.biceps.setcontextstateoperationstate_pb2 import SetContextStateOperationStateMsg
from org.somda.protosdc.proto.model.biceps.setmetricstateoperationstate_pb2 import SetMetricStateOperationStateMsg
from org.somda.protosdc.proto.model.biceps.setstringoperationstate_pb2 import SetStringOperationStateMsg
from org.somda.protosdc.proto.model.biceps.setvalueoperationstate_pb2 import SetValueOperationStateMsg
from org.somda.protosdc.proto.model.biceps.stringmetricstate_pb2 import StringMetricStateMsg
from org.somda.protosdc.proto.model.biceps.systemcontextstate_pb2 import SystemContextStateMsg
from org.somda.protosdc.proto.model.biceps.vmdstate_pb2 import VmdStateMsg
from sdc11073.mdib import statecontainers as sc

from .generic import generic_from_p, generic_to_p
from .mapping_helpers import name_to_p, p_name_from_pm_name, find_populated_one_of, is_one_of_msg

_logger = logging.getLogger('sdc.grpc.map.state')

_to_cls = {}
_to_cls[sc.SetValueOperationStateContainer] = SetValueOperationStateMsg
_to_cls[sc.MdsStateContainer] = MdsStateMsg
_to_cls[sc.SetStringOperationStateContainer] = SetStringOperationStateMsg
_to_cls[sc.ActivateOperationStateContainer] = ActivateOperationStateMsg
_to_cls[sc.SetContextStateOperationStateContainer] = SetContextStateOperationStateMsg
_to_cls[sc.SetMetricStateOperationStateContainer] = SetMetricStateOperationStateMsg
_to_cls[sc.SetComponentStateOperationStateContainer] = SetComponentStateOperationStateMsg
_to_cls[sc.SetAlertStateOperationStateContainer] = SetAlertStateOperationStateMsg
_to_cls[sc.NumericMetricStateContainer] = NumericMetricStateMsg
_to_cls[sc.StringMetricStateContainer] = StringMetricStateMsg
_to_cls[sc.EnumStringMetricStateContainer] = EnumStringMetricStateMsg
_to_cls[sc.RealTimeSampleArrayMetricStateContainer] = RealTimeSampleArrayMetricStateMsg
_to_cls[sc.DistributionSampleArrayMetricStateContainer] = DistributionSampleArrayMetricStateMsg
_to_cls[sc.ScoStateContainer] = ScoStateMsg
_to_cls[sc.VmdStateContainer] = VmdStateMsg
_to_cls[sc.ChannelStateContainer] = ChannelStateMsg
_to_cls[sc.ClockStateContainer] = ClockStateMsg
_to_cls[sc.SystemContextStateContainer] = SystemContextStateMsg
_to_cls[sc.BatteryStateContainer] = BatteryStateMsg
_to_cls[sc.AlertSystemStateContainer] = AlertSystemStateMsg
_to_cls[sc.AlertSignalStateContainer] = AlertSignalStateMsg
_to_cls[sc.AlertConditionStateContainer] = AlertConditionStateMsg
_to_cls[sc.LimitAlertConditionStateContainer] = LimitAlertConditionStateMsg
_to_cls[sc.LocationContextStateContainer] = LocationContextStateMsg
_to_cls[sc.PatientContextStateContainer] = PatientContextStateMsg
_to_cls[sc.AbstractComplexDeviceComponentStateContainer] = AbstractComplexDeviceComponentStateMsg

# invert for other direction lookup
_from_cls = dict((v, k) for (k, v) in _to_cls.items())


def find_one_of_state(p):
    fields = p.ListFields()
    if len(fields) != 1:
        raise ValueError(f'p has {len(fields)} fields, expect exactly one')
    return fields[0][1]


def generic_state_to_p(state, p):
    if p is None:
        cls = _to_cls[state.__class__]
        p = cls()
    generic_to_p(state, p)
    return p


def _p_walk(p, ret=None):
    if ret is None:
        ret = []
    if is_one_of_msg(p):
        fields = p.ListFields()
        for field in fields:
            ret.extend(_p_walk(field[1]))
    else:
        pm_cls = _from_cls[p.__class__]
        classes = inspect.getmro(pm_cls)
        p_current_entry_point = None
        for tmp_cls in classes:
            if tmp_cls.__name__.startswith('_'):
                # convention: if a class name starts with underscore, it is not part of biceps inheritance hierarchy
                continue
            try:
                names = tmp_cls._props  # pylint: disable=protected-access
            except:
                continue
            # determine p_current_entry_point
            if p_current_entry_point is None:
                p_current_entry_point = p
            else:
                # find parent class members entry point
                p_name = name_to_p(tmp_cls.__name__)
                p_current_entry_point = getattr(p_current_entry_point, p_name)
            ret.append((tmp_cls, p_current_entry_point, names))
    return ret


def p_get_attr_value(p, pm_attr_name):
    for tmp_cls, p_current_entry_point, names in _p_walk(p):
        if pm_attr_name in names:
            p_name = p_name_from_pm_name(p_current_entry_point, tmp_cls, pm_attr_name)
            value = getattr(p_current_entry_point, p_name)
            return value


def _state_from_oneof_p(p, nsmap, descr):
    p_field = find_populated_one_of(p)
    cls = _from_cls[p_field.__class__]
    ret = cls(nsmap, descr)
    generic_from_p(p_field, ret)
    return ret


def generic_state_from_p(p, descr):
    p_field = find_populated_one_of(p)
    cls = _from_cls[p_field.__class__]
    ret = cls(descr)
    generic_from_p(p_field, ret)
    return ret
