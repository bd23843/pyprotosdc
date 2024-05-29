from __future__ import annotations

import decimal
import logging
from typing import List, Callable

from google.protobuf.wrappers_pb2 import Int32Value
from lxml import etree as etree_
from org.somda.protosdc.proto.model.biceps.abstractstateoneof_pb2 import AbstractStateOneOfMsg
from org.somda.protosdc.proto.model.biceps.basedemographics_pb2 import BaseDemographicsMsg
from org.somda.protosdc.proto.model.biceps.basedemographicsoneof_pb2 import (BaseDemographicsOneOfMsg,
                                                                             PersonReferenceOneOfMsg)
from org.somda.protosdc.proto.model.biceps.instanceidentifier_pb2 import InstanceIdentifierMsg
from org.somda.protosdc.proto.model.biceps.instanceidentifieroneof_pb2 import InstanceIdentifierOneOfMsg
from org.somda.protosdc.proto.model.biceps.localizedtext_pb2 import LocalizedTextMsg
from org.somda.protosdc.proto.model.biceps.operatingjurisdiction_pb2 import OperatingJurisdictionMsg
from org.somda.protosdc.proto.model.biceps.patientdemographicscoredataoneof_pb2 import \
    PatientDemographicsCoreDataOneOfMsg
from org.somda.protosdc.proto.model.biceps.qualifiedname_pb2 import QualifiedNameMsg
from org.somda.protosdc.proto.model.biceps.qualityindicator_pb2 import QualityIndicatorMsg
from org.somda.protosdc.proto.model.biceps.realtimevaluetype_pb2 import RealTimeValueTypeMsg
from org.somda.protosdc.proto.model.biceps.referencedversion_pb2 import ReferencedVersionMsg
from org.somda.protosdc.proto.model.biceps.samplearrayvalue_pb2 import SampleArrayValueMsg
from org.somda.protosdc.proto.model.biceps.setstringoperationstate_pb2 import SetStringOperationStateMsg
from org.somda.protosdc.proto.model.biceps.versioncounter_pb2 import VersionCounterMsg
from org.somda.protosdc.proto.model.common import common_types_pb2
from sdc11073.mdib.containerbase import ContainerBase
from sdc11073.mdib.statecontainers import (AllowedValuesType,
                                           AbstractStateContainer)
from sdc11073.xml_types.pm_types import BaseDemographics, PatientDemographicsCoreData
from sdc11073.xml_types.pm_types import ApplyAnnotation
from sdc11073.xml_types.pm_types import InstanceIdentifier, \
    OperatingJurisdiction
from sdc11073.xml_types.pm_types import LocalizedText, CodedValue
from sdc11073.xml_types.pm_types import NeonatalPatientDemographicsCoreData, PersonReference
from sdc11073.xml_types.pm_types import PersonParticipation
from sdc11073.xml_types.pm_types import PropertyBasedPMType
from sdc11073.xml_types.xml_structure import (ExtensionLocalValue
                                              )

from .basic_mappers import (string_value_to_p, string_value_from_p,
                            decimal_to_p, decimal_from_p,
                            enum_attr_to_p, enum_attr_from_p_func)
from .mapping_helpers import (attr_name_to_p,
                              p_name_from_pm_name,
                              find_one_of_p_for_container)


# _logger() = logging.getLogger('sdc.grpc.map.pmtypes')

def _logger():
    return logging.getLogger('sdc.grpc.map.pmtypes')


_stop_iter_classes = (PropertyBasedPMType, ContainerBase, ExtensionLocalValue)


def quality_indicator_to_p(pm_value: decimal.Decimal,
                           # pm_property_type: type,
                           p_dest: QualityIndicatorMsg):
    decimal_to_p(pm_value, p_dest.decimal)


def version_counter_to_p(pm_value: int,
                         # pm_property_type: type,
                         p_dest: VersionCounterMsg):
    p_dest.unsigned_long = pm_value


def version_counter_from_p(p_src: VersionCounterMsg):
    return p_src.unsigned_long


def integer_to_p(pm_value: int,
                 # pm_property_type: type,
                 p_dest: Int32Value):
    p_dest.value = pm_value


def any_uri_to_p(pm_value: int,
                 # pm_property_type: type,
                 p_dest: Int32Value):
    p_dest.any_u_r_i = pm_value


def referenced_version_to_p(pm_value: int,
                            # pm_property_type: type,
                            p_dest: ReferencedVersionMsg):
    p_dest.version_counter.unsigned_long = pm_value


def instance_identifier_to_oneof_p_func(inst: [InstanceIdentifier, OperatingJurisdiction],
                                        p: InstanceIdentifierOneOfMsg,
                                        recurse_func: Callable,
                                        recurse_count: int) -> InstanceIdentifierOneOfMsg:
    if isinstance(inst, OperatingJurisdiction):
        tmp = OperatingJurisdictionMsg()
        recurse_func(inst, tmp, recurse_count)
        p.operating_jurisdiction.CopyFrom(tmp)
    elif isinstance(inst, InstanceIdentifier):
        tmp = InstanceIdentifierMsg()
        recurse_func(inst, tmp, recurse_count)
        p.instance_identifier.CopyFrom(tmp)
    else:
        raise TypeError(f'instance_identifier_to_oneof_p cannot handle cls {inst.__class__}')
    which = p.WhichOneof(p.DESCRIPTOR.oneofs[0].name)
    if which is None:
        raise ValueError(f'could not anything in {p.__class__.__name__}')
    return p


def instance_identifier_to_oneof_p(inst: [InstanceIdentifier, OperatingJurisdiction],
                                   recurse_func: Callable,
                                   recurse_count: int) -> InstanceIdentifierOneOfMsg:
    p = InstanceIdentifierOneOfMsg()
    instance_identifier_to_oneof_p_func(inst, p, recurse_func, recurse_count)
    return p


def instance_identifier_from_oneof_p(p: InstanceIdentifierOneOfMsg,
                                     recurse_func: Callable,
                                     recurse_count: int) -> [InstanceIdentifier, OperatingJurisdiction, None]:
    which = p.WhichOneof(p.DESCRIPTOR.oneofs[0].name)
    if which == 'instance_identifier':
        ret = InstanceIdentifier(None)
        recurse_func(p.instance_identifier, ret, recurse_count)
        return ret
    if which == 'operating_jurisdiction':
        ret = OperatingJurisdiction(None)
        recurse_func(p.operating_jurisdiction, ret, recurse_count)
        return ret
    return None


def _realtime_array_to_p(samples: List[str],
                         p: RealTimeValueTypeMsg,
                         recurse_count: int) -> None:
    for s in samples:
        tmp = common_types_pb2.Decimal()
        decimal_to_p(s, tmp)
        p.decimal.append(tmp)


def _realtime_array_from_p(p: RealTimeValueTypeMsg) -> list[decimal.Decimal]:
    return [decimal_from_p(d) for d in p.decimal]
    # return [DecimalConverter.to_py(sc) for sc in p.real_time_value_type]


def _base_demographics_to_p(bd: BaseDemographics, p: BaseDemographicsMsg) -> BaseDemographicsMsg:
    if p is None:
        p = BaseDemographicsMsg()
    string_value_to_p(bd.Givenname, p.givenname)
    string_value_to_p(bd.Familyname, p.familyname)
    string_value_to_p(bd.Birthname, p.birthname)
    p.middlename.extend(bd.Middlename)
    string_value_to_p(bd.Title, p.title)
    return p


def base_demographics_to_oneof_p_func(bd: BaseDemographics,
                                      p: BaseDemographicsOneOfMsg,
                                      recurse_func: Callable,
                                      recurse_count: int) -> BaseDemographicsOneOfMsg:
    if isinstance(bd, NeonatalPatientDemographicsCoreData):
        recurse_func(bd, p.neonatal_patient_demographics_core_data, recurse_count)
    elif isinstance(bd, PatientDemographicsCoreData):
        recurse_func(bd, p.patient_demographics_core_data, recurse_count)
    else:
        # generic_to_p does not work, Middlename (list of strings <-> list of strings ) cannot be handled safely
        _base_demographics_to_p(bd, p.base_demographics)
    return p


def base_demographics_to_oneof_p(bd: BaseDemographics,
                                 recurse_func: Callable,
                                 recurse_count: int) -> BaseDemographicsOneOfMsg:
    if isinstance(bd, PatientDemographicsCoreData):
        p = PatientDemographicsCoreDataOneOfMsg()
    else:
        p = BaseDemographicsOneOfMsg()

    base_demographics_to_oneof_p_func(bd, p, recurse_func, recurse_count)
    return p


def base_demographics_from_oneof_p(p: BaseDemographicsOneOfMsg,
                                   recurse_func: Callable,
                                   recurse_count: int) -> BaseDemographics:
    which = p.WhichOneof(p.DESCRIPTOR.oneofs[0].name)
    if which == 'base_demographics':
        p_src = p.base_demographics
        ret = BaseDemographics()
        ret.Givenname = string_value_from_p(p_src, 'givenname')
        ret.Familyname = string_value_from_p(p_src, 'familyname')
        ret.Birthname = string_value_from_p(p_src, 'birthname')
        ret.Middlename.extend(p_src.middlename)
        ret.Title = string_value_from_p(p_src, 'title')
        return ret
    elif which == 'neonatal_patient_demographics_core_data':
        ret = NeonatalPatientDemographicsCoreData()
        return recurse_func(p.neonatal_patient_demographics_core_data, ret, recurse_count +1)
    elif which == 'patient_demographics_core_data':
        ret = PatientDemographicsCoreData()
        return recurse_func(p.patient_demographics_core_data, ret, recurse_count +1)


def person_reference_to_oneof_p_func(pr: PersonReference,
                                     p: PersonReferenceOneOfMsg,
                                     recurse_func: Callable,
                                     recurse_count: int) -> PersonReferenceOneOfMsg:
    if isinstance(pr, PersonParticipation):
        recurse_func(pr, p.person_participation, recurse_count)
    elif isinstance(pr, PersonReference):
        recurse_func(pr, p.person_reference, recurse_count)
    else:
        raise TypeError(f'cannot convert {pr.__class__.__name__}')
    return p


def person_reference_to_oneof_p(pr: PersonReference,
                                recurse_func: Callable,
                                recurse_count: int) -> PersonReferenceOneOfMsg:
    p = PersonReferenceOneOfMsg()
    person_reference_to_oneof_p_func(pr, p, recurse_func, recurse_count)
    return p


def person_reference_from_oneof_p(p: PersonReferenceOneOfMsg,
                                  recurse_func: Callable,
                                  recurse_count: int) -> PersonReference | None:
    which = p.WhichOneof(p.DESCRIPTOR.oneofs[0].name)
    if which == 'person_participation':
        return recurse_func(p.person_participation, None, recurse_count + 1)
    if which == 'person_reference':
        return recurse_func(p.person_reference, None, recurse_count + 1)
    return None


def allowed_values_from_p_func(p: SetStringOperationStateMsg.AllowedValuesMsg,
                               recurse_func: Callable,
                               recurse_count: int
                               ) -> AllowedValuesType:
    aw = AllowedValuesType()
    for value in p.value:
        aw.Value.append(value)
    return aw


def node_text_qname_to_p(q_name: etree_.QName, p: QualifiedNameMsg):
    # no _to_special_func!
    p.namespace = q_name.namespace
    p.local_name = q_name.localname


def node_text_qname_from_p(p: QualifiedNameMsg) -> etree_.QName:
    # no _from_special_func!
    return etree_.QName(p.namespace, p.local_name)


# def coded_value_to_p_func(coded_value: CodedValue,
#                           p: CodedValueMsg,
#                           recurse_count: int) -> CodedValueMsg:
#     tmp = getattr(p, attr_name_to_p("Code"))
#     tmp.string = coded_value.Code
#
#     string_value_to_p(coded_value.CodingSystem, getattr(p, attr_name_to_p("CodingSystem")))
#
#     if coded_value.CodingSystemVersion:
#         string_value_to_p(coded_value.CodingSystemVersion, getattr(p, attr_name_to_p("CodingSystemVersion")))
#
#     p_list = getattr(p, p_name_from_pm_name(p, coded_value, "ConceptDescription"))
#     for c in coded_value.ConceptDescription:
#         p_list.append(localized_text_to_p(c, recurse_count))
#
#     p_list = getattr(p, p_name_from_pm_name(p, coded_value, "CodingSystemName"))
#     for c in coded_value.CodingSystemName:
#         p_list.append(localized_text_to_p(c, recurse_count))
#
#     if coded_value.SymbolicCodeName:
#         tmp = getattr(p, attr_name_to_p("SymbolicCodeName"))
#         tmp.string = coded_value.SymbolicCodeName
#     # Todo: ExtEntension
#
#     p_list = getattr(p, p_name_from_pm_name(p, coded_value, "Translation"))
#     for t in coded_value.Translation:
#         trans = CodedValueMsg.TranslationMsg()
#         tmp = getattr(trans, attr_name_to_p("Code"))
#         tmp.string = t.Code
#
#         string_value_to_p(t.CodingSystem, getattr(trans, attr_name_to_p("CodingSystem")))
#
#         if t.CodingSystemVersion:
#             string_value_to_p(t.CodingSystemVersion, getattr(trans, attr_name_to_p("CodingSystemVersion")))
#         # Todo: ExtEntension
#         p_list.append(trans)


def localized_text_to_p_func(ltext: LocalizedText,
                             p: LocalizedTextMsg,
                             recurse_count: int) -> LocalizedTextMsg:
    p.localized_text_content.string = ltext.text  # the text itself
    if ltext.Lang is not None:
        p_name = attr_name_to_p('Lang')
        string_value_to_p(ltext.Lang, getattr(p, p_name))
    if ltext.Ref is not None:
        p_name = attr_name_to_p('Ref')
        getattr(p, p_name).string = ltext.Ref
    if ltext.TextWidth is not None:
        p_name = attr_name_to_p('TextWidth')
        enum_attr_to_p(ltext.TextWidth, getattr(p, p_name))
    if ltext.Version is not None:
        p_name = attr_name_to_p('Version')
        version_attr = getattr(p, p_name)
        version_attr.version_counter.unsigned_long = ltext.Version
    return p


def localized_text_to_p(ltext: LocalizedText,
                        recurse_func: Callable,
                        recurse_count: int) -> LocalizedTextMsg:
    p = LocalizedTextMsg()
    return localized_text_to_p_func(ltext, p, recurse_count)


def localized_text_from_p(p: LocalizedTextMsg,
                          recurse_func: Callable,
                          recurse_count: int) -> LocalizedText:
    ltext = LocalizedText(p.localized_text_content.string)
    p_name = attr_name_to_p('Lang')
    if p.HasField(p_name):
        ltext.Lang = string_value_from_p(p, p_name)

    p_name = attr_name_to_p('Ref')
    if p.HasField(p_name):
        ltext.Ref = getattr(p, p_name).string

    p_name = attr_name_to_p('TextWidth')
    if p.HasField(p_name):
        ltext.TextWidth = enum_attr_from_p_func(p, p_name, ltext, 'TextWidth')

    p_name = attr_name_to_p('Version')
    if p.HasField(p_name):
        ltext.Version = getattr(p, p_name).version_counter.unsigned_long
    return ltext


def apply_annotation_to_p_func(apply_anno: ApplyAnnotation,
                               p: SampleArrayValueMsg.ApplyAnnotationMsg,
                               recurse_func: Callable,
                               recurse_count: int) -> SampleArrayValueMsg.ApplyAnnotationMsg:
    p_name = attr_name_to_p('AnnotationIndex')
    setattr(p, p_name, apply_anno.AnnotationIndex)
    p_name = attr_name_to_p('SampleIndex')
    setattr(p, p_name, apply_anno.SampleIndex)
    return p


def state_to_one_of_p_func(state: AbstractStateContainer,
                           p_dest: AbstractStateOneOfMsg,
                           recurse_func: Callable,
                           recurse_count: int) -> AbstractStateOneOfMsg:
    p_field = find_one_of_p_for_container(state, p_dest)
    recurse_func(state, p_field, recurse_count + 1)
    return p_dest


# def metric_state_to_one_of_p_func(state: AbstractMetricStateContainer,
#                            p_dest: AbstractMetricStateOneOfMsg,
#                            recurse_count: int) -> AbstractMetricStateOneOfMsg:
#     p_field = find_one_of_p_for_container(state, p_dest)
#     generic_to_p(state, p_field)
#     return p_dest
#
# def alert_state_to_one_of_p_func(state: AbstractAlertStateContainer,
#                            p_dest: AbstractAlertStateOneOfMsg,
#                            recurse_count: int) -> AbstractAlertStateOneOfMsg:
#     p_field = find_one_of_p_for_container(state, p_dest)
#     generic_to_p(state, p_field)
#     return p_dest

def apply_annotation_to_p(apply_anno: ApplyAnnotation,
                          recurse_func: Callable,
                          recurse_count: int) -> SampleArrayValueMsg.ApplyAnnotationMsg:
    p = SampleArrayValueMsg.ApplyAnnotationMsg()
    apply_annotation_to_p_func(apply_anno, p, recurse_func, recurse_count)
    return p


def apply_annotation_from_p(p: SampleArrayValueMsg.ApplyAnnotationMsg, pm_dest: ApplyAnnotation) -> ApplyAnnotation:
    # if pm_dest is None:
    #     pm_dest = ApplyAnnotation()
    p_name = attr_name_to_p('AnnotationIndex')
    pm_dest.AnnotationIndex = getattr(p, p_name)
    p_name = attr_name_to_p('SampleIndex')
    pm_dest.SampleIndex = getattr(p, p_name)
    return pm_dest
