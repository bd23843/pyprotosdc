from __future__ import annotations

import inspect
import logging
from typing import Any, Callable

from google.protobuf.internal.python_message import GeneratedProtocolMessageType
from org.somda.protosdc.proto.model.biceps.abstractalertstateoneof_pb2 import AbstractAlertStateOneOfMsg
from org.somda.protosdc.proto.model.biceps.abstractcontextstateoneof_pb2 import AbstractContextStateOneOfMsg
from org.somda.protosdc.proto.model.biceps.abstractdevicecomponentdescriptor_pb2 import \
    AbstractDeviceComponentDescriptorMsg
from org.somda.protosdc.proto.model.biceps.abstractdevicecomponentstateoneof_pb2 import \
    AbstractDeviceComponentStateOneOfMsg
from org.somda.protosdc.proto.model.biceps.abstractmetricdescriptor_pb2 import AbstractMetricDescriptorMsg
from org.somda.protosdc.proto.model.biceps.abstractmetricstateoneof_pb2 import AbstractMetricStateOneOfMsg
from org.somda.protosdc.proto.model.biceps.abstractmetricvalue_pb2 import AbstractMetricValueMsg
from org.somda.protosdc.proto.model.biceps.abstractstateoneof_pb2 import AbstractStateOneOfMsg
from org.somda.protosdc.proto.model.biceps.activateoperationdescriptor_pb2 import ActivateOperationDescriptorMsg
from org.somda.protosdc.proto.model.biceps.basedemographics_pb2 import BaseDemographicsMsg
from org.somda.protosdc.proto.model.biceps.basedemographicsoneof_pb2 import (BaseDemographicsOneOfMsg,
                                                                             PersonReferenceMsg,
                                                                             PersonReferenceOneOfMsg,
                                                                             PersonParticipationMsg)
from org.somda.protosdc.proto.model.biceps.causeinfo_pb2 import CauseInfoMsg
from org.somda.protosdc.proto.model.biceps.clinicalinfo_pb2 import ClinicalInfoMsg
from org.somda.protosdc.proto.model.biceps.codedvalue_pb2 import CodedValueMsg
from org.somda.protosdc.proto.model.biceps.enumstringmetricdescriptor_pb2 import EnumStringMetricDescriptorMsg
from org.somda.protosdc.proto.model.biceps.handleref_pb2 import HandleRefMsg
from org.somda.protosdc.proto.model.biceps.imagingprocedure_pb2 import ImagingProcedureMsg
from org.somda.protosdc.proto.model.biceps.instanceidentifier_pb2 import InstanceIdentifierMsg
from org.somda.protosdc.proto.model.biceps.instanceidentifieroneof_pb2 import InstanceIdentifierOneOfMsg
from org.somda.protosdc.proto.model.biceps.localizedtext_pb2 import LocalizedTextMsg
from org.somda.protosdc.proto.model.biceps.locationdetail_pb2 import LocationDetailMsg
from org.somda.protosdc.proto.model.biceps.locationreference_pb2 import LocationReferenceMsg
from org.somda.protosdc.proto.model.biceps.mdsdescriptor_pb2 import MdsDescriptorMsg
from org.somda.protosdc.proto.model.biceps.measurement_pb2 import MeasurementMsg
from org.somda.protosdc.proto.model.biceps.numericmetricvalue_pb2 import NumericMetricValueMsg
from org.somda.protosdc.proto.model.biceps.operatingjurisdiction_pb2 import OperatingJurisdictionMsg
from org.somda.protosdc.proto.model.biceps.orderdetail_pb2 import OrderDetailMsg
from org.somda.protosdc.proto.model.biceps.patientdemographicscoredata_pb2 import PatientDemographicsCoreDataMsg
from org.somda.protosdc.proto.model.biceps.patientdemographicscoredataoneof_pb2 import \
    PatientDemographicsCoreDataOneOfMsg
from org.somda.protosdc.proto.model.biceps.physicalconnectorinfo_pb2 import PhysicalConnectorInfoMsg
from org.somda.protosdc.proto.model.biceps.range_pb2 import RangeMsg
from org.somda.protosdc.proto.model.biceps.realtimevaluetype_pb2 import RealTimeValueTypeMsg
from org.somda.protosdc.proto.model.biceps.remedyinfo_pb2 import RemedyInfoMsg
from org.somda.protosdc.proto.model.biceps.samplearrayvalue_pb2 import SampleArrayValueMsg
from org.somda.protosdc.proto.model.biceps.scostate_pb2 import ScoStateMsg
from org.somda.protosdc.proto.model.biceps.setstringoperationstate_pb2 import SetStringOperationStateMsg
from org.somda.protosdc.proto.model.biceps.stringmetricvalue_pb2 import StringMetricValueMsg
from org.somda.protosdc.proto.model.biceps.systemsignalactivation_pb2 import SystemSignalActivationMsg
from org.somda.protosdc.proto.model.biceps.transactionid_pb2 import TransactionIdMsg
from org.somda.protosdc.proto.model.biceps.workflowcontextstate_pb2 import WorkflowContextStateMsg
from org.somda.protosdc.proto.model.common import common_types_pb2
from sdc11073.mdib.containerbase import ContainerBase
from sdc11073.mdib.statecontainers import (AllowedValuesType)
from sdc11073.xml_types.pm_types import AllowedValue, BaseDemographics, PatientDemographicsCoreData
from sdc11073.xml_types.pm_types import ApplyAnnotation, CauseInfo, RemedyInfo, ActivateOperationDescriptorArgument
from sdc11073.xml_types.pm_types import LocalizedText, CodedValue, TranslationType, Annotation
from sdc11073.xml_types.pm_types import MetricQualityType, NumericMetricValue, StringMetricValue, SampleArrayValue
from sdc11073.xml_types.pm_types import NeonatalPatientDemographicsCoreData, PersonReference, LocationDetail, \
    LocationReference
from sdc11073.xml_types.pm_types import OrderDetail, PerformedOrderDetail, WorkflowDetail
from sdc11073.xml_types.pm_types import PropertyBasedPMType
from sdc11073.xml_types.pm_types import Range, Measurement, PersonParticipation, PhysicalConnectorInfo
from sdc11073.xml_types.pm_types import ReferenceRange, RelatedMeasurement, ClinicalInfo, ImagingProcedure
from sdc11073.xml_types.pm_types import Relation, UdiType
from sdc11073.xml_types.pm_types import RequestedOrderDetail, MetaData, OperationGroup
from sdc11073.xml_types.pm_types import SystemSignalActivation, ProductionSpecification, InstanceIdentifier, \
    OperatingJurisdiction
from sdc11073.xml_types.xml_structure import (_AttributeBase,
                                              _AttributeListBase,
                                              CodeIdentifierAttributeProperty,
                                              StringAttributeProperty,
                                              DecimalAttributeProperty,
                                              QualityIndicatorAttributeProperty,
                                              AnyURIAttributeProperty,
                                              SymbolicCodeNameAttributeProperty,
                                              ExtensionAttributeProperty,
                                              TimestampAttributeProperty,
                                              CurrentTimestampAttributeProperty,
                                              IntegerAttributeProperty,
                                              UnsignedIntAttributeProperty,
                                              DurationAttributeProperty,
                                              EnumAttributeProperty,
                                              BooleanAttributeProperty,
                                              HandleAttributeProperty,
                                              HandleRefAttributeProperty,
                                              LocalizedTextRefAttributeProperty,
                                              DecimalListAttributeProperty,
                                              SubElementListProperty,
                                              SubElementWithSubElementListProperty,
                                              OperationRefListAttributeProperty,
                                              EntryRefListAttributeProperty,
                                              AlertConditionRefListAttributeProperty,
                                              ReferencedVersionAttributeProperty,
                                              VersionCounterAttributeProperty,
                                              ExtensionLocalValue
                                              )
from sdc11073.xml_types.xml_structure import (_ElementListProperty,
                                              NodeTextProperty,
                                              NodeEnumTextProperty,
                                              NodeTextQNameProperty,
                                              SubElementTextListProperty,
                                              SubElementHandleRefListProperty
                                              )

from .basic_mappers import (duration_to_p, duration_from_p,
                            decimal_to_p, decimal_from_p,
                            enum_attr_to_p, enum_attr_from_p_func)
from .mapping_helpers import (name_to_p,
                              attr_name_to_p,
                              p_name_from_pm_name)
from .pmtypesmapper import (instance_identifier_to_oneof_p_func,
                            base_demographics_to_oneof_p_func,
                            person_reference_to_oneof_p_func,
                            state_to_one_of_p_func,
                            instance_identifier_to_oneof_p,
                            person_reference_to_oneof_p,
                            base_demographics_to_oneof_p,
                            localized_text_to_p,
                            apply_annotation_to_p,
                            quality_indicator_to_p,
                            referenced_version_to_p,
                            version_counter_to_p,
                            integer_to_p,
                            any_uri_to_p,
                            allowed_values_from_p_func,
                            instance_identifier_from_oneof_p,
                            person_reference_from_oneof_p,
                            base_demographics_from_oneof_p,
                            _realtime_array_from_p,
                            localized_text_from_p,
                            node_text_qname_to_p,
                            node_text_qname_from_p)


# _logger() = logging.getLogger('sdc.grpc.map.pmtypes')

def _logger():
    return logging.getLogger('sdc.grpc.map.pmtypes')


_stop_iter_classes = (PropertyBasedPMType, ContainerBase, ExtensionLocalValue, object)

_to_cls = {}
_to_cls[Relation] = AbstractMetricDescriptorMsg.RelationMsg
_to_cls[PerformedOrderDetail] = WorkflowContextStateMsg.WorkflowDetailMsg.PerformedOrderDetailMsg
_to_cls[RequestedOrderDetail] = WorkflowContextStateMsg.WorkflowDetailMsg.RequestedOrderDetailMsg
_to_cls[ClinicalInfo] = ClinicalInfoMsg
_to_cls[LocalizedText] = LocalizedTextMsg
_to_cls[PersonParticipation] = PersonParticipationMsg
_to_cls[ImagingProcedure] = ImagingProcedureMsg
_to_cls[CodedValue] = CodedValueMsg
_to_cls[WorkflowDetail] = WorkflowContextStateMsg.WorkflowDetailMsg
_to_cls[LocationReference] = LocationReferenceMsg
_to_cls[LocationDetail] = LocationDetailMsg
_to_cls[PersonReference] = PersonReferenceMsg
_to_cls[InstanceIdentifier] = InstanceIdentifierMsg
_to_cls[OperatingJurisdiction] = OperatingJurisdictionMsg
_to_cls[Range] = RangeMsg
_to_cls[MetricQualityType] = AbstractMetricValueMsg.MetricQualityMsg
_to_cls[NumericMetricValue] = NumericMetricValueMsg
_to_cls[StringMetricValue] = StringMetricValueMsg
_to_cls[SampleArrayValue] = SampleArrayValueMsg
_to_cls[Annotation] = AbstractMetricValueMsg.AnnotationMsg
_to_cls[ApplyAnnotation] = SampleArrayValueMsg.ApplyAnnotationMsg
_to_cls[CauseInfo] = CauseInfoMsg
_to_cls[RemedyInfo] = RemedyInfoMsg
_to_cls[ActivateOperationDescriptorArgument] = ActivateOperationDescriptorMsg.ArgumentMsg
_to_cls[PhysicalConnectorInfo] = PhysicalConnectorInfoMsg
_to_cls[SystemSignalActivation] = SystemSignalActivationMsg
_to_cls[ProductionSpecification] = AbstractDeviceComponentDescriptorMsg.ProductionSpecificationMsg
_to_cls[BaseDemographics] = BaseDemographicsMsg
_to_cls[PatientDemographicsCoreData] = PatientDemographicsCoreDataMsg
_to_cls[ReferenceRange] = ClinicalInfoMsg.RelatedMeasurementMsg.ReferenceRangeMsg
_to_cls[RelatedMeasurement] = ClinicalInfoMsg.RelatedMeasurementMsg
_to_cls[OrderDetail] = OrderDetailMsg
_to_cls[UdiType] = MdsDescriptorMsg.MetaDataMsg.UdiMsg
_to_cls[AllowedValue] = EnumStringMetricDescriptorMsg.AllowedValueMsg
_to_cls[TranslationType] = CodedValueMsg.TranslationMsg
_to_cls[Measurement] = MeasurementMsg
_to_cls[MetaData] = MdsDescriptorMsg.MetaDataMsg
_to_cls[OperationGroup] = ScoStateMsg.OperationGroupMsg
_to_cls[AllowedValuesType] = SetStringOperationStateMsg.AllowedValuesMsg

# invert for other direction lookup
_from_cls = dict((v, k) for (k, v) in _to_cls.items())

# these functions copy data from a pm type to a protobuf type
to_p_func_type = Callable[
    [PropertyBasedPMType, GeneratedProtocolMessageType, Callable, int], GeneratedProtocolMessageType]

_to_p_funcs_by_pm: dict[Any, to_p_func_type] = {
    # LocalizedText: localized_text_to_p_func,
    # CodedValue: coded_value_to_p_func,
}


_to_one_of_p_funcs_by_p: dict[Any, to_p_func_type] = {
    InstanceIdentifierOneOfMsg: instance_identifier_to_oneof_p_func,
    BaseDemographicsOneOfMsg: base_demographics_to_oneof_p_func,
    PatientDemographicsCoreDataOneOfMsg: base_demographics_to_oneof_p_func,
    PersonReferenceOneOfMsg: person_reference_to_oneof_p_func,
    AbstractStateOneOfMsg: state_to_one_of_p_func,
    AbstractMetricStateOneOfMsg: state_to_one_of_p_func,
    AbstractAlertStateOneOfMsg: state_to_one_of_p_func,
    AbstractContextStateOneOfMsg: state_to_one_of_p_func,
    AbstractDeviceComponentStateOneOfMsg: state_to_one_of_p_func,
}

# these functions return a new protobuf type instance from the given pm type argument
to_p_factory_type = Callable[[PropertyBasedPMType, Callable, int], GeneratedProtocolMessageType]

_to_p_factories_by_pm: dict[Any, to_p_factory_type] = {
    InstanceIdentifier: instance_identifier_to_oneof_p,
    PersonReference: person_reference_to_oneof_p,
    BaseDemographics: base_demographics_to_oneof_p,
    PatientDemographicsCoreData: base_demographics_to_oneof_p,
    NeonatalPatientDemographicsCoreData: base_demographics_to_oneof_p,
    LocalizedText: localized_text_to_p,
    ApplyAnnotation: apply_annotation_to_p
}

attr_to_p_funcs: dict[Any, Callable[[Any, Any], None]] = {
    EnumAttributeProperty: enum_attr_to_p,
    NodeEnumTextProperty: enum_attr_to_p,
    DurationAttributeProperty: duration_to_p,
    QualityIndicatorAttributeProperty: quality_indicator_to_p,
    DecimalAttributeProperty: decimal_to_p,
    ReferencedVersionAttributeProperty: referenced_version_to_p,
    VersionCounterAttributeProperty: version_counter_to_p,
    IntegerAttributeProperty: integer_to_p,
    UnsignedIntAttributeProperty: integer_to_p,
    AnyURIAttributeProperty: any_uri_to_p
}

# these functions return a new pm type instance from the given protobuf argument
from_p_factory_type = Callable[[GeneratedProtocolMessageType, Callable, int], PropertyBasedPMType]

_from_p_factories: dict[Any, from_p_factory_type] = {
    AllowedValuesType: allowed_values_from_p_func  # needed because AllowedValue has no is_optional member.
}


_from_one_of_p_factories: dict[Any, from_p_factory_type] = {
    InstanceIdentifierOneOfMsg: instance_identifier_from_oneof_p,
    PersonReferenceOneOfMsg: person_reference_from_oneof_p,
    BaseDemographicsOneOfMsg: base_demographics_from_oneof_p,
    PatientDemographicsCoreDataOneOfMsg: base_demographics_from_oneof_p,
    RealTimeValueTypeMsg: _realtime_array_from_p,
    LocalizedTextMsg: localized_text_from_p,
}


def generic_to_p(pm_src: PropertyBasedPMType,
                 p_dest: GeneratedProtocolMessageType | None,
                 ):
    """If there is a specific handler for pm_src, use it to create a proto buf instance with all data from pm_src.
    If not, create an empty protobuf instance that matches pm_src and call map_generic_to_p to copty data to it.
    """
    if p_dest is None:
        # is there a special handler for whole pm_src class?
        special_handler = _to_p_factories_by_pm.get(pm_src.__class__)
        if special_handler:
            _logger().debug('special handling cls=%s, %s', pm_src.__class__.__name__, special_handler.__name__)
            p_dest = special_handler(pm_src, map_generic_to_p, recurse_count=0)
            _logger().debug('special handling cls=%s done', pm_src.__class__.__name__)
            return p_dest
        else:
            p_dest = _to_cls[pm_src.__class__]()
    return map_generic_to_p(pm_src, p_dest, 0)


def map_generic_to_p(pm_src: PropertyBasedPMType,
                     p_dest: GeneratedProtocolMessageType,
                     recurse_count: int,
                     ):
    """Copy data from a pm_types object to a proto object.
     :param pm_src: the pm_types object to copy from
     :param p_dest: the proto object to copy to.
     :param recurse_count: used to calculate indent for log output
    """

    indent = '     ' * recurse_count

    # is there a special handler for whole pm_src class?
    special_handler_pm = _to_p_funcs_by_pm.get(pm_src.__class__)
    if special_handler_pm:
        _logger().debug('%s special handling cls=%s, %s', indent, pm_src.__class__.__name__,
                        special_handler_pm.__name__)
        special_handler_pm(pm_src, p_dest, map_generic_to_p, recurse_count + 1)
        _logger().debug('%s special handling cls=%s done', indent, pm_src.__class__.__name__)
        return p_dest

    # is there a special handler for whole p_dest class?
    to_one_of_p_func = _to_one_of_p_funcs_by_p.get(p_dest.__class__)
    if to_one_of_p_func:
        _logger().debug('%s special handling cls=%s, %s', indent, p_dest.__class__.__name__, to_one_of_p_func.__name__)
        to_one_of_p_func(pm_src, p_dest, map_generic_to_p, recurse_count + 1)
        _logger().debug('%s special handling cls=%s done', indent, p_dest.__class__.__name__)
        return p_dest

    p_current_entry_point = None
    classes = inspect.getmro(pm_src.__class__)
    for tmp_cls in classes:
        if tmp_cls.__name__.startswith('_'):
            # convention: if a class name starts with underscore, it is not part of biceps inheritance hierarchy
            continue
        elif tmp_cls is ExtensionLocalValue:
            # Todo: handle extensions
            continue
        elif tmp_cls in _stop_iter_classes:
            # this is a python base class, has nothing to do with biceps classes. stop here
            break
        try:
            pm_prop_names = tmp_cls.__dict__['_props']  # this checks only current class, not parent
        except KeyError:
            pm_prop_names = []
            # continue
        _logger().debug('%s handling class %s', indent, tmp_cls.__name__)
        # determine p_current_entry_point
        if p_current_entry_point is None:
            p_current_entry_point = p_dest
        else:
            # find parent class members entry point
            p_name = name_to_p(tmp_cls.__name__)
            try:
                p_current_entry_point = getattr(p_current_entry_point, p_name)
            except AttributeError as ex:
                raise
        # iterate over all properties
        for pm_prop_name in pm_prop_names:
            cp_type = getattr(pm_src.__class__, pm_prop_name)
            _logger().debug('%s handling %s, cls=%s', indent, tmp_cls.__name__, cp_type.__class__.__name__)
            # special handler for property?
            special_handler = _to_p_funcs_by_pm.get(cp_type.__class__)
            if special_handler:
                _logger().debug('%s special handling %s = %s', indent, pm_src.__class__.__name__,
                                special_handler.__name__)
                special_handler(pm_src, p_current_entry_point)
                _logger().debug('%s special handling %s done', indent, pm_src.__class__.__name__)
                continue

            value = getattr(pm_src, pm_prop_name)
            if value in (None, []):
                continue

            # determine member name in p:
            if isinstance(cp_type, _AttributeBase):
                p_name = attr_name_to_p(pm_prop_name)
            else:
                p_name = p_name_from_pm_name(p_current_entry_point, pm_src, pm_prop_name)

            # convert
            try:
                p_dest_current = getattr(p_current_entry_point, p_name)
            except AttributeError as ex:
                raise

            to_one_of_p_func = _to_one_of_p_funcs_by_p.get(p_dest_current.__class__)
            if to_one_of_p_func is not None:
                _logger().debug('%s special p_dest handling %s = %s', indent, p_dest_current.__class__.__name__,
                                to_one_of_p_func.__name__)
                to_one_of_p_func(value, p_dest_current, map_generic_to_p, recurse_count)
                _logger().debug('%s special p_dest handling %s done', indent, p_dest_current.__class__.__name__)
                continue

            try:
                attr_to_p_funcs[cp_type.__class__](value, p_dest_current)
                continue
            except KeyError:
                pass
            # handle some types that cannot be handled with the simple call above
            if isinstance(cp_type, BooleanAttributeProperty):
                if cp_type.is_optional:
                    p_dest_current.value = value
                else:
                    setattr(p_dest, p_name, value)
            elif isinstance(cp_type, TimestampAttributeProperty):
                p_dest_current.unsigned_long = int(cp_type._converter.to_xml(value))
            elif isinstance(cp_type, CurrentTimestampAttributeProperty):
                p_dest_current.unsigned_long = int(cp_type._converter.to_xml(value))
            elif isinstance(cp_type, OperationRefListAttributeProperty):
                pm_list = getattr(pm_src, pm_prop_name)
                if pm_list is not None:
                    for ref in pm_list:
                        tmp = HandleRefMsg()
                        tmp.string = ref
                        p_dest_current.handle_ref.append(tmp)
            elif isinstance(cp_type, EntryRefListAttributeProperty):
                pm_list = getattr(pm_src, pm_prop_name)
                if pm_list is not None:
                    for ref in pm_list:
                        tmp = HandleRefMsg()
                        tmp.string = ref
                        p_dest_current.handle_ref.append(tmp)

            elif isinstance(cp_type, AlertConditionRefListAttributeProperty):
                pm_list = getattr(pm_src, pm_prop_name)
                if pm_list is not None:
                    for ref in pm_list:
                        tmp = HandleRefMsg()
                        tmp.string = ref
                        p_dest_current.handle_ref.append(tmp)
            elif isinstance(cp_type, DecimalListAttributeProperty):
                pm_list = getattr(pm_src, pm_prop_name)
                if pm_list is not None:
                    for dec in pm_list:
                        tmp = common_types_pb2.Decimal()
                        decimal_to_p(dec, tmp)
                        p_dest_current.decimal.append(tmp)

            elif isinstance(cp_type, _AttributeListBase):
                # This is always a list of handles ( values in 'entry_ref')
                pm_list = getattr(pm_src, pm_prop_name)
                if pm_list is not None:
                    try:
                        p_dest_current.handle_ref.extend(pm_list)
                    except AttributeError as ex:
                        raise
            elif isinstance(cp_type, _AttributeBase):
                # type conversion if needed
                p_value = cp_type._converter.to_xml(value)
                try:
                    if cp_type.is_optional:
                        if hasattr(p_dest_current, 'value'):
                            p_dest_current.value = p_value
                        elif hasattr(p_dest_current, 'string'):
                            p_dest_current.string = p_value
                        else:
                            raise AttributeError(f'do not know how to handle {p_dest_current.__class__}')
                    else:
                        tmp = getattr(p_current_entry_point, p_name)
                        tmp.string = p_value
                except AttributeError:
                    raise
            elif isinstance(cp_type, NodeTextQNameProperty):
                if cp_type.is_optional:
                    raise TypeError(f'no handler for optional {p_dest_current.__class__.__name__} ')
                else:
                    node_text_qname_to_p(value, getattr(p_current_entry_point, p_name))
            elif isinstance(cp_type, NodeTextProperty):
                if cp_type.is_optional:
                    p_dest_current.value = value
                else:
                    setattr(p_current_entry_point, p_name, value)
            elif isinstance(cp_type, SubElementListProperty):
                # In case of a list we might need a factory method
                for elem in value:
                    special_handler_list = _to_p_factories_by_pm.get(elem.__class__)
                    if special_handler_list:
                        _logger().debug('%s special pm_src handling %s = %s', indent, elem.__class__.__name__,
                                        special_handler_list.__name__)
                        p_value = special_handler_list(elem, map_generic_to_p, recurse_count)
                        _logger().debug('%s special pm_src handling %s done', indent, elem.__class__.__name__)
                    else:
                        p_value = _to_cls[elem.__class__]()

                        _logger().debug('%s recursive list elem generic_to_p(%s, %s)',
                                        indent, elem.__class__.__name__, p_value.__class__.__name__)
                        map_generic_to_p(elem, p_value, recurse_count + 1)
                        # p_value = _generic_to_p(elem, None, recurse_count + 1)
                        _logger().debug('%s recursive list elem generic_to_p(%s, %s) done',
                                        indent, elem.__class__.__name__, p_value.__class__.__name__)
                    _logger().debug('%s add %s to list %s.%s',
                                    indent, p_value.__class__.__name__,
                                    p_current_entry_point.__class__.__name__,
                                    p_name)
                    p_dest_current.append(p_value)
            elif isinstance(cp_type, SubElementHandleRefListProperty):
                pm_list = getattr(pm_src, pm_prop_name)
                for elem in pm_list:
                    tmp = HandleRefMsg()
                    tmp.string = elem
                    p_dest_current.append(tmp)
            elif isinstance(cp_type, SubElementTextListProperty):
                p_dest_current.extend(value)
            else:
                _logger().debug('%s recursive generic_to_p(%s, %s)', indent, value.__class__.__name__,
                                p_dest_current.__class__.__name__)
                map_generic_to_p(value, p_dest_current, recurse_count + 1)
                _logger().debug('%s recursive generic_to_p done', indent)
    return p_dest


def generic_from_p(p: GeneratedProtocolMessageType,
                   pm_dest: PropertyBasedPMType | None = None):
    return map_generic_from_p(p, pm_dest, 0)


def map_generic_from_p(p: GeneratedProtocolMessageType,
                       pm_dest: PropertyBasedPMType | None,
                       recurse_count: int):
    indent = '     ' * recurse_count

    if pm_dest is None:
        pm_factory = _from_one_of_p_factories.get(p.__class__)
        if pm_factory:
            _logger().debug('%s special factory for class %s = %s', indent, p.__class__.__name__, pm_factory.__name__)
            return pm_factory(p, map_generic_from_p, recurse_count + 1)

        _logger().debug('%s generic instantiate class %s', indent, p.__class__.__name__)
        # use inspect to determine number of parameters for constructor.
        # then call constructor with all parameters = None
        try:
            pm_cls = _from_cls[p.__class__]
        except KeyError as ex:
            raise
        sig = inspect.signature(pm_cls.__init__)
        args = [None] * (len(sig.parameters) - 1)
        pm_dest = pm_cls(*args)
    classes = inspect.getmro(pm_dest.__class__)
    _logger().debug('%s inheritance = %r', indent, [c.__name__ for c in classes])
    p_current_entry_point = None
    for tmp_cls in classes:
        if tmp_cls.__name__.startswith('_'):
            # convention: if a class name starts with underscore, it is not part of biceps inheritance hierarchy
            continue
        elif tmp_cls in _stop_iter_classes:
            # this is a python base class, has nothing to do with biceps classes. stop here
            break
        try:
            names = tmp_cls.__dict__['_props']  # this checks only current class, not parent
        except KeyError:
            names = []
        _logger().debug('%s handling tmp_cls %s', indent, tmp_cls.__name__)
        # determine p_current_entry_point
        if p_current_entry_point is None:
            p_current_entry_point = p
        else:
            # find parent class members entry point
            p_name = name_to_p(tmp_cls.__name__)
            p_current_entry_point = getattr(p_current_entry_point, p_name)
        # special handler for whole dest class?
        special_handler = _from_p_factories.get(tmp_cls)
        if special_handler:
            _logger().debug('%s special handling tmp_cls %s = %s', indent, tmp_cls.__name__, special_handler.__name__)
            special_handler(p_current_entry_point, pm_dest)
            _logger().debug('%s special handling tmp_cls %s done', indent, tmp_cls.__name__)
            break

        # iterate over all properties
        for name in names:
            _logger().debug('%s handling %s.%s', indent, tmp_cls.__name__, name)
            dest_type = getattr(pm_dest.__class__, name)

            if isinstance(dest_type, SubElementWithSubElementListProperty):
                # This is only a helper class, the real class is in value_class
                dest_type = dest_type.value_class

            # determine p_name and p_src
            if isinstance(dest_type, _AttributeBase):
                p_name = attr_name_to_p(name)
                if isinstance(dest_type, _AttributeListBase):
                    p_src = getattr(p_current_entry_point, p_name)
                else:
                    if dest_type.is_optional and not p_current_entry_point.HasField(p_name):
                        # optional value, not set
                        continue
                    p_src = getattr(p_current_entry_point, p_name)
            else:
                p_name = p_name_from_pm_name(p, pm_dest.__class__, name)
                if isinstance(dest_type, _ElementListProperty):
                    p_src = getattr(p_current_entry_point, p_name)
                else:
                    try:
                        if hasattr(dest_type,
                                   'is_optional') and dest_type.is_optional and not p_current_entry_point.HasField(p_name):
                            # optional value, not set
                            continue
                    except:
                        raise
                    p_src = getattr(p_current_entry_point, p_name)

            # special handler for dest property?
            special_handler = _from_p_factories.get(dest_type)
            if special_handler:
                _logger().debug('%s special handling dest_type %s = %s', indent, dest_type.__name__,
                                special_handler.__name__)
                value = special_handler(p_src, map_generic_from_p, recurse_count + 1)
                setattr(pm_dest, name, value)
                _logger().debug('%s special handling tmp_cls %s done', indent, tmp_cls.__name__)

            elif isinstance(dest_type, _AttributeListBase):
                p_name = attr_name_to_p(name)
                p_src = getattr(p_current_entry_point, p_name)

                if isinstance(dest_type, AlertConditionRefListAttributeProperty):
                    dest_list = getattr(pm_dest, name)
                    for elem in p_src.handle_ref:
                        dest_list.append(elem.string)
                elif isinstance(dest_type, OperationRefListAttributeProperty):
                    scr_list = getattr(p_current_entry_point, p_name)
                    dest_list = []
                    if scr_list.handle_ref:
                        setattr(pm_dest, name, dest_list)
                        for src in scr_list.handle_ref:
                            dest_list.append(src.string)
                elif isinstance(dest_type, EntryRefListAttributeProperty):
                    scr_list = getattr(p_current_entry_point, p_name)
                    dest_list = getattr(pm_dest, name)
                    if scr_list.handle_ref:
                        for src in scr_list.handle_ref:
                            dest_list.append(src.string)
                elif isinstance(dest_type, DecimalListAttributeProperty):
                    scr_list = getattr(p_current_entry_point, p_name)
                    dest_list = getattr(pm_dest, name)
                    for src in scr_list.decimal:
                        dest_list.append(decimal_from_p(src))

                else:  # _AttributeListBase):
                    # This is always a list of handles
                    dest_list = getattr(pm_dest, name)
                    try:
                        for elem in p_src.entry_ref:
                            dest_list.append(elem)
                    except AttributeError as ex:
                        raise
            elif isinstance(dest_type, _AttributeBase):
                p_name = attr_name_to_p(name)
                if dest_type.is_optional and not p_current_entry_point.HasField(p_name):
                    # optional value, not set
                    continue
                p_src = getattr(p_current_entry_point, p_name)
                if isinstance(dest_type, EnumAttributeProperty):
                    if p_current_entry_point.HasField(p_name):
                        value = enum_attr_from_p_func(p_current_entry_point, p_name, pm_dest, name)
                        setattr(pm_dest, name, value)
                elif isinstance(dest_type, ReferencedVersionAttributeProperty):
                    if not dest_type.is_optional or (dest_type.is_optional and p_current_entry_point.HasField(p_name)):
                        setattr(pm_dest, name, p_src.version_counter.unsigned_long)
                elif isinstance(dest_type, VersionCounterAttributeProperty):
                    if not dest_type.is_optional or (dest_type.is_optional and p_current_entry_point.HasField(p_name)):
                        setattr(pm_dest, name, p_src.unsigned_long)
                elif isinstance(dest_type, DurationAttributeProperty):
                    value = duration_from_p(p_src)
                    setattr(pm_dest, name, value)
                elif isinstance(dest_type, QualityIndicatorAttributeProperty):
                    value = decimal_from_p(p_src.decimal)
                    setattr(pm_dest, name, value)
                elif isinstance(dest_type, DecimalAttributeProperty):
                    value = decimal_from_p(p_src)
                    setattr(pm_dest, name, value)
                elif isinstance(dest_type, (TimestampAttributeProperty, CurrentTimestampAttributeProperty)):
                    value = p_src.unsigned_long
                    setattr(pm_dest, name, value / 1000)
                elif isinstance(dest_type, (HandleAttributeProperty,
                                            HandleRefAttributeProperty,
                                            LocalizedTextRefAttributeProperty,
                                            SymbolicCodeNameAttributeProperty,
                                            ExtensionAttributeProperty)):
                    setattr(pm_dest, name, p_src.string)
                elif isinstance(dest_type, AnyURIAttributeProperty):
                    setattr(pm_dest, name, p_src.any_u_r_i)
                elif isinstance(dest_type, CodeIdentifierAttributeProperty):
                    # Code is never optional
                    setattr(pm_dest, name, p_src.string)
                elif isinstance(dest_type, (BooleanAttributeProperty, IntegerAttributeProperty)):
                    # handle optional / non-optional fields
                    try:
                        value = p_src.value if dest_type.is_optional else p_src
                        if value is not None:
                            setattr(pm_dest, name, value)
                    except AttributeError as ex:
                        raise
                elif isinstance(dest_type, StringAttributeProperty):
                    # handle optional / non-optional fields
                    try:
                        value = p_src.value if dest_type.is_optional else p_src
                        if value is not None:
                            setattr(pm_dest, name, value)
                    except AttributeError as ex:
                        raise
                else:
                    raise RuntimeError(f'{dest_type}')
            elif isinstance(dest_type, _ElementListProperty):
                p_name = p_name_from_pm_name(p, pm_dest.__class__, name)
                p_src = getattr(p_current_entry_point, p_name)

                if isinstance(dest_type, SubElementListProperty):
                    dest_list = getattr(pm_dest, name)
                    for elem in p_src:
                        pm_factory = _from_one_of_p_factories.get(elem.__class__)
                        if pm_factory:
                            _logger().debug('%s special list elem handling %s = %s', indent, elem.__class__.__name__,
                                            pm_factory.__name__)
                            pm_value = pm_factory(elem, map_generic_from_p, recurse_count + 1)
                            dest_list.append(pm_value)
                            _logger().debug('%s special list elem handling %s done', indent, elem.__class__.__name__)
                        else:
                            _logger().debug('%s recursive list elem map_generic_from_p(%s)', indent,
                                            elem.__class__.__name__)
                            pm_value = map_generic_from_p(elem, None, recurse_count + 1)
                            dest_list.append(pm_value)
                            _logger().debug('%s recursive list elem map_generic_from_p %s = %s done', indent,
                                            pm_dest.__class__.__name__, pm_value)
                elif isinstance(dest_type, SubElementHandleRefListProperty):
                    dest_list = getattr(pm_dest, name)
                    for handle_ref_msg in p_src:
                        dest_list.append(handle_ref_msg.string)

                elif isinstance(dest_type, SubElementTextListProperty):
                    dest_list = getattr(pm_dest, name)
                    dest_list.extend(p_src)
                elif isinstance(dest_type, SubElementWithSubElementListProperty):
                    dest_list = getattr(pm_dest, name)
                    dest_list.extend(p_src)

            else:  # this is a single biceps sub node, it can be optional
                # determine member name in p:
                p_name = p_name_from_pm_name(p, pm_dest.__class__, name)

                try:
                    if dest_type.is_optional and not p_current_entry_point.HasField(p_name):
                        # optional value, not set
                        continue
                except:
                    raise
                p_src = getattr(p_current_entry_point, p_name)

                pm_factory = _from_one_of_p_factories.get(p_src.__class__)

                if pm_factory:
                    _logger().debug('%s special handling src_cls  %s = %s', indent, p_src.__class__.__name__,
                                    pm_factory.__name__)
                    value = pm_factory(p_src, map_generic_from_p, recurse_count + 1)
                    setattr(pm_dest, name, value)
                    _logger().debug('%s special handling src_cls  %s done', indent, p_src.__class__.__name__)
                    continue
                elif isinstance(p_src, TransactionIdMsg):
                    setattr(pm_dest, name, p_src.unsigned_int)
                elif isinstance(dest_type, NodeTextQNameProperty):
                    qname = node_text_qname_from_p(p_src)
                    setattr(pm_dest, name, qname)
                elif isinstance(dest_type, NodeEnumTextProperty):
                    if p_current_entry_point.HasField(p_name):
                        value = enum_attr_from_p_func(p_current_entry_point, p_name, pm_dest, name)
                        setattr(pm_dest, name, value)
                elif isinstance(dest_type, NodeTextProperty):
                    str_value = p_src if not dest_type.is_optional else p_src.value
                    setattr(pm_dest, name, str_value)
                else:
                    if p_current_entry_point.HasField(p_name):
                        _logger().debug('%s recursive map_generic_from_p(%s, %s)', indent, p_src.__class__.__name__,
                                        pm_dest.__class__.__name__)
                        value = map_generic_from_p(p_src, None, recurse_count + 1)
                        setattr(pm_dest, name, value)
                        _logger().debug('%s recursive map_generic_from_p done', indent)
    return pm_dest
