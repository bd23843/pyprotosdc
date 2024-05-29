import logging

from org.somda.protosdc.proto.model.biceps.activateoperationdescriptor_pb2 import ActivateOperationDescriptorMsg
from org.somda.protosdc.proto.model.biceps.alertconditiondescriptor_pb2 import AlertConditionDescriptorMsg
from org.somda.protosdc.proto.model.biceps.alertsignaldescriptor_pb2 import AlertSignalDescriptorMsg
from org.somda.protosdc.proto.model.biceps.alertsystemdescriptor_pb2 import AlertSystemDescriptorMsg
from org.somda.protosdc.proto.model.biceps.batterydescriptor_pb2 import BatteryDescriptorMsg
from org.somda.protosdc.proto.model.biceps.channeldescriptor_pb2 import ChannelDescriptorMsg
from org.somda.protosdc.proto.model.biceps.clockdescriptor_pb2 import ClockDescriptorMsg
from org.somda.protosdc.proto.model.biceps.distributionsamplearraymetricdescriptor_pb2 import \
    DistributionSampleArrayMetricDescriptorMsg
from org.somda.protosdc.proto.model.biceps.ensemblecontextdescriptor_pb2 import EnsembleContextDescriptorMsg
from org.somda.protosdc.proto.model.biceps.enumstringmetricdescriptor_pb2 import EnumStringMetricDescriptorMsg
from org.somda.protosdc.proto.model.biceps.limitalertconditiondescriptor_pb2 import LimitAlertConditionDescriptorMsg
from org.somda.protosdc.proto.model.biceps.locationcontextdescriptor_pb2 import LocationContextDescriptorMsg
from org.somda.protosdc.proto.model.biceps.mdsdescriptor_pb2 import MdsDescriptorMsg
from org.somda.protosdc.proto.model.biceps.meanscontextdescriptor_pb2 import MeansContextDescriptorMsg
from org.somda.protosdc.proto.model.biceps.numericmetricdescriptor_pb2 import NumericMetricDescriptorMsg
from org.somda.protosdc.proto.model.biceps.operatorcontextdescriptor_pb2 import OperatorContextDescriptorMsg
from org.somda.protosdc.proto.model.biceps.patientcontextdescriptor_pb2 import PatientContextDescriptorMsg
from org.somda.protosdc.proto.model.biceps.realtimesamplearraymetricdescriptor_pb2 import \
    RealTimeSampleArrayMetricDescriptorMsg
from org.somda.protosdc.proto.model.biceps.scodescriptor_pb2 import ScoDescriptorMsg
from org.somda.protosdc.proto.model.biceps.setalertstateoperationdescriptor_pb2 import \
    SetAlertStateOperationDescriptorMsg
from org.somda.protosdc.proto.model.biceps.setcomponentstateoperationdescriptor_pb2 import \
    SetComponentStateOperationDescriptorMsg
from org.somda.protosdc.proto.model.biceps.setcontextstateoperationdescriptor_pb2 import \
    SetContextStateOperationDescriptorMsg
from org.somda.protosdc.proto.model.biceps.setmetricstateoperationdescriptor_pb2 import \
    SetMetricStateOperationDescriptorMsg
from org.somda.protosdc.proto.model.biceps.setstringoperationdescriptor_pb2 import SetStringOperationDescriptorMsg
from org.somda.protosdc.proto.model.biceps.setvalueoperationdescriptor_pb2 import SetValueOperationDescriptorMsg
from org.somda.protosdc.proto.model.biceps.stringmetricdescriptor_pb2 import StringMetricDescriptorMsg
from org.somda.protosdc.proto.model.biceps.systemcontextdescriptor_pb2 import SystemContextDescriptorMsg
from org.somda.protosdc.proto.model.biceps.vmddescriptor_pb2 import VmdDescriptorMsg
from org.somda.protosdc.proto.model.biceps.workflowcontextdescriptor_pb2 import WorkflowContextDescriptorMsg
from sdc11073.mdib import descriptorcontainers as dc

from .generic import generic_from_p, generic_to_p
from .mapping_helpers import find_one_of_p_for_container, find_populated_one_of, is_one_of_msg

_logger = logging.getLogger('sdc.grpc.map.descriptor')

_to_cls = {}
_to_cls[dc.MdsDescriptorContainer] = MdsDescriptorMsg
_to_cls[dc.VmdDescriptorContainer] = VmdDescriptorMsg
_to_cls[dc.ChannelDescriptorContainer] = ChannelDescriptorMsg
_to_cls[dc.ScoDescriptorContainer] = ScoDescriptorMsg
_to_cls[dc.ClockDescriptorContainer] = ClockDescriptorMsg
_to_cls[dc.BatteryDescriptorContainer] = BatteryDescriptorMsg
_to_cls[dc.NumericMetricDescriptorContainer] = NumericMetricDescriptorMsg
_to_cls[dc.StringMetricDescriptorContainer] = StringMetricDescriptorMsg
_to_cls[dc.EnumStringMetricDescriptorContainer] = EnumStringMetricDescriptorMsg
_to_cls[dc.RealTimeSampleArrayMetricDescriptorContainer] = RealTimeSampleArrayMetricDescriptorMsg
_to_cls[dc.DistributionSampleArrayMetricDescriptorContainer] = DistributionSampleArrayMetricDescriptorMsg
_to_cls[dc.SetValueOperationDescriptorContainer] = SetValueOperationDescriptorMsg
_to_cls[dc.SetStringOperationDescriptorContainer] = SetStringOperationDescriptorMsg
_to_cls[dc.SetContextStateOperationDescriptorContainer] = SetContextStateOperationDescriptorMsg
_to_cls[dc.SetMetricStateOperationDescriptorContainer] = SetMetricStateOperationDescriptorMsg
_to_cls[dc.SetComponentStateOperationDescriptorContainer] = SetComponentStateOperationDescriptorMsg
_to_cls[dc.SetAlertStateOperationDescriptorContainer] = SetAlertStateOperationDescriptorMsg
_to_cls[dc.ActivateOperationDescriptorContainer] = ActivateOperationDescriptorMsg
_to_cls[dc.AlertSystemDescriptorContainer] = AlertSystemDescriptorMsg
_to_cls[dc.AlertConditionDescriptorContainer] = AlertConditionDescriptorMsg
_to_cls[dc.LimitAlertConditionDescriptorContainer] = LimitAlertConditionDescriptorMsg
_to_cls[dc.AlertSignalDescriptorContainer] = AlertSignalDescriptorMsg
_to_cls[dc.PatientContextDescriptorContainer] = PatientContextDescriptorMsg
_to_cls[dc.LocationContextDescriptorContainer] = LocationContextDescriptorMsg
_to_cls[dc.WorkflowContextDescriptorContainer] = WorkflowContextDescriptorMsg
_to_cls[dc.OperatorContextDescriptorContainer] = OperatorContextDescriptorMsg
_to_cls[dc.MeansContextDescriptorContainer] = MeansContextDescriptorMsg
_to_cls[dc.EnsembleContextDescriptorContainer] = EnsembleContextDescriptorMsg
_to_cls[dc.SystemContextDescriptorContainer] = SystemContextDescriptorMsg

# invert for other direction lookup
_from_cls = dict((v, k) for (k, v) in _to_cls.items())


def generic_descriptor_to_p(descr, p):
    try:
        if p is None:
            cls = _to_cls[descr.__class__]
            p = cls()
        if is_one_of_msg(p):
            p2 = find_one_of_p_for_container(descr, p)
            generic_to_p(descr, p2)
            return p2
        generic_to_p(descr, p)
        return p
    except:
        raise


def generic_descriptor_from_p(p, parent_handle):
    p_field = find_populated_one_of(p)
    cls = _from_cls[p_field.__class__]
    ret = cls(None, parent_handle)
    generic_from_p(p_field, ret)
    return ret
