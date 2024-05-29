import unittest
import logging
from math import isclose
from decimal import Decimal
from sdc11073.xml_types import pm_types
from sdc11073.namespaces import default_ns_helper as nsh
from sdc11073.loghelper import basic_logging_setup
from pyprotosdc.mapping import pmtypesmapper, generic


def diff(a: pm_types.PropertyBasedPMType, b:pm_types.PropertyBasedPMType) ->dict:
    ret = {}
    for name, dummy in a.sorted_container_properties():
        try:
            a_value = getattr(a, name)
            b_value = getattr(b, name)
            if a_value == b_value:
                continue
            elif (isinstance(a_value, float) or isinstance(b_value, float)) and isclose(a_value, b_value):
                continue  # float compare (almost equal)
            else:
                ret[name] = (a_value, b_value)
        except (TypeError, AttributeError) as ex:
            ret[name] = ex
    return ret

def _start_logger():
    logger = logging.getLogger('sdc.grpc.map')
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    # create formatter
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    # add formatter to ch
    ch.setFormatter(formatter)
    # add ch to logger
    logger.addHandler(ch)
    return ch

def _stop_logger(handler):
    logger = logging.getLogger('sdc.grpc')
    logger.setLevel(logging.WARNING)
    logger.removeHandler(handler)


class TestPmtypesMapper(unittest.TestCase):
    def setUp(self) -> None:
        basic_logging_setup('sdc')
        logging.getLogger('sdc.grpc.map').setLevel(logging.DEBUG)
        # self._log_handler = _start_logger()

    def tearDown(self) -> None:
        pass
        # _stop_logger(self._log_handler)

    def check_convert(self, obj):
        obj_p = generic.generic_to_p(obj, None)
        print('\n################################# generic_from_p##################')
        obj2 = generic.generic_from_p(obj_p)
        delta = diff(obj, obj2)
        if delta:
            print(f'delta = {delta}')
        self.assertEqual(obj, obj2)

    def test_localized_text(self):
        l_min = pm_types.LocalizedText('foo')
        l_max = pm_types.LocalizedText('foo',
                                       lang='de_eng',
                                       ref='abc',
                                       version=42,
                                       text_width=pm_types.LocalizedTextWidth.M)
        for obj in l_min, l_max:
            self.check_convert(obj)

    def test_codedvalue(self):
        c_min = pm_types.CodedValue('123')
        c_max = pm_types.CodedValue('345', 'foo', '42',
                                   coding_system_names=[pm_types.LocalizedText('csn')],
                                   concept_descriptions=[pm_types.LocalizedText('cd')],
                                   symbolic_code_name='scn'
                                    )
        c_max.Translation.append(pm_types.Translation('trans', coding_system_version='v33'))
        # c_max.Translation.append(pm_types.T_Translation('code', 'codingsystem', codingSystemVersion='12'))
        for obj in c_min, c_max:
            self.check_convert(obj)

    def test_instance_identifier(self):
        # inst_min = pm_types.InstanceIdentifier('my_root')
        # self.check_convert(inst_min)
        inst_max = pm_types.InstanceIdentifier('my_root',
                                              pm_types.CodedValue('abc', 'def'),
                                              [pm_types.LocalizedText('xxx'), pm_types.LocalizedText('yyy')],
                                              'ext_string')
        self.check_convert(inst_max)
        op_min = pm_types.OperatingJurisdiction('my_root')
        self.check_convert(op_min)
        op_max = pm_types.OperatingJurisdiction('my_root',
                                              pm_types.CodedValue('abc', 'def'),
                                              [pm_types.LocalizedText('xxx'), pm_types.LocalizedText('yyy')],
                                              'ext_string')
        self.check_convert(op_max)

    def test_range(self):
        rg_max = pm_types.Range(Decimal('0.01'), Decimal('42'), Decimal('0.1'), Decimal('0.2'), Decimal('0.3'))
        rg_min = pm_types.Range()
        for obj in (rg_max, rg_min):
            self.check_convert(obj)

    def test_measurement(self):
        obj = pm_types.Measurement(Decimal('42'), pm_types.CodedValue('abc'))
        self.check_convert(obj)

    def test_allowed_value(self):
        aw_max = pm_types.AllowedValue('an_allowed_value', pm_types.CodedValue('abc'))
        aw_min = pm_types.AllowedValue('another_allowed_value')
        for obj in (aw_max, aw_min):
            self.check_convert(obj)

    def test_numericmetricvalue(self):
        n_min = pm_types.NumericMetricValue()
        n_max = pm_types.NumericMetricValue()
        n_max.DeterminationTime = 12345
        n_max.MetricQuality.Mode = pm_types.GenerationMode.TEST
        n_max.MetricQuality.Qi = Decimal('2.111')
        n_max.MetricQuality.Validity= pm_types.MeasurementValidity.CALIBRATION_ONGOING
        n_max.StartTime = 999
        n_max.StopTime = 9999
        n_max.Value = Decimal('0.9')
        annot = pm_types.Annotation(pm_types.CodedValue('a','b'))
        n_max.Annotation.append(annot)
        for obj in n_max, n_min:
            self.check_convert(obj)

    def test_stringmetricvalue(self):
        s_min = pm_types.StringMetricValue()
        s_max = pm_types.StringMetricValue()
        s_max.DeterminationTime = 12345
        s_max.MetricQuality.Mode = pm_types.GenerationMode.TEST
        s_max.MetricQuality.Qi = Decimal('2.111')
        s_max.MetricQuality.Validity= pm_types.MeasurementValidity.CALIBRATION_ONGOING
        s_max.StartTime = 999
        s_max.StopTime = 9999
        s_max.Value = 'hey!'
        annot = pm_types.Annotation(pm_types.CodedValue('a','b'))
        s_max.Annotation.append(annot)
        for obj in s_max, s_min:
            self.check_convert(obj)


    def test_samplearrayvalue(self):
        s_min = pm_types.SampleArrayValue()
        s_max = pm_types.SampleArrayValue()
        s_max.DeterminationTime = 12345
        s_max.MetricQuality.Mode = pm_types.GenerationMode.TEST
        s_max.MetricQuality.Qi = Decimal('2.111')
        s_max.StartTime = 999
        s_max.StopTime = 9999
        s_max.MetricQuality.Validity= pm_types.MeasurementValidity.CALIBRATION_ONGOING
        s_max.Samples = [Decimal('1'), Decimal('2.2')]
        annot = pm_types.Annotation(pm_types.CodedValue('a','b'))
        s_max.Annotation.append(annot)
        apply_annot = pm_types.ApplyAnnotation(1, 1)
        s_max.ApplyAnnotation.append(apply_annot)

        for obj in [s_max, s_min]:
            self.check_convert(obj)

    def test_cause_info(self):
        ci_max = pm_types.CauseInfo(pm_types.RemedyInfo([pm_types.LocalizedText('rembla')]),
                                   [pm_types.LocalizedText('caubla',
                                                           lang='de',
                                                           ref='x',
                                                           text_width=pm_types.LocalizedTextWidth.XXL,
                                                           version=2)])
        ci_min = pm_types.CauseInfo(None, [])
        for obj in [ci_max, ci_min]:
            self.check_convert(obj)

    def test_argument(self):
        obj = pm_types.ActivateOperationDescriptorArgument(pm_types.CodedValue('aaa'), nsh.PM.tag('oh'))
        self.check_convert(obj)

    def test_physical_connector_info(self):
        pci_max = pm_types.PhysicalConnectorInfo([pm_types.LocalizedText('foo'), pm_types.LocalizedText('bar')], 42)
        pci_min = pm_types.PhysicalConnectorInfo([], None)
        for obj in (pci_max, pci_min):
            self.check_convert(obj)

    def test_system_signal_activation(self):
        obj = pm_types.SystemSignalActivation(pm_types.AlertSignalManifestation.AUD,
                                              pm_types.AlertActivation.PAUSED)
        self.check_convert(obj)

    def test_production_specification(self):
        ps_min = pm_types.ProductionSpecification(pm_types.CodedValue('abc', 'def'), 'prod_spec')
        ps_max = pm_types.ProductionSpecification(pm_types.CodedValue('abc', 'def'), 'prod_spec',
                                                 pm_types.InstanceIdentifier('my_root',
                                                                            pm_types.CodedValue('xxx', 'yyy')))
        for obj in (ps_max, ps_min):
            self.check_convert(obj)

    def test_base_demographics(self):
        bd_max = pm_types.BaseDemographics()
        bd_max.Givenname = 'Charles'
        bd_max.Middlename = ['M.']
        bd_max.Familyname = 'Schulz'
        bd_max.Birthname = 'Meyer'
        bd_max.Title = 'Dr.'
        bd_min = pm_types.BaseDemographics()
        for obj in (bd_max, bd_min):
            self.check_convert(obj)

    def test_patient_demographics_core_data(self):
        bd_max = pm_types.PatientDemographicsCoreData()
        bd_max.Givenname = 'Charles'
        bd_max.Middlename = ['M.']
        bd_max.Familyname = 'Schulz'
        bd_max.Birthname = 'Meyer'
        bd_max.Title = 'Dr.'
        bd_max.Sex = pm_types.Sex.FEMALE
        bd_max.Height = pm_types.Measurement(Decimal('1.88'), pm_types.CodedValue('height_code'))
        bd_min = pm_types.PatientDemographicsCoreData()
        bd_min.Givenname = 'Charles'  # at least one member must be set., otherwise mapping fails
        for obj in (bd_max, bd_min):
            self.check_convert(obj)

    def test_neonatal_patient_demographics_core_data(self):
        bd_max = pm_types.NeonatalPatientDemographicsCoreData()
        bd_max.Givenname = 'Charles'
        bd_max.Middlename = ['M.']
        bd_max.Familyname = 'Schulz'
        bd_max.Birthname = 'Meyer'
        bd_max.Title = 'Dr.'
        bd_max.Sex = pm_types.Sex.FEMALE
        bd_max.Height = pm_types.Measurement(Decimal('0.41'), pm_types.CodedValue('height_code'))
        bd_max.HeadCircumference = pm_types.Measurement(Decimal('0.25'), pm_types.CodedValue('circum_code'))
        bd_min = pm_types.NeonatalPatientDemographicsCoreData()
        bd_min.Givenname = 'Charles'  # at least one member must be set., otherwise mapping fails
        for obj in (bd_max, bd_min):
            self.check_convert(obj)

    def test_location_detail(self):
        loc_max = pm_types.LocationDetail('poc', 'room', 'bed', 'facility', 'building', 'floor')
        loc_min = pm_types.LocationDetail()
        for obj in (loc_max, loc_min):
            self.check_convert(obj)

    def test_location_reference(self):
        loc_max = pm_types.LocationReference(identifications=[pm_types.InstanceIdentifier('root'),
                                                             pm_types.InstanceIdentifier('root2')],
                                            location_detail=pm_types.LocationDetail('poc', 'room', 'bed'))
        loc_min = pm_types.LocationReference()
        for obj in (loc_max, loc_min):
            self.check_convert(obj)

    def test_person_reference(self):
        pr_max = pm_types.PersonReference([pm_types.InstanceIdentifier('root'), pm_types.InstanceIdentifier('root2')],
                                         name=pm_types.BaseDemographics('Charles', ['M.'], 'Schulz', 'Meyer', 'Dr.'))
        # schema has no minOccurs => at least one InstanceIdentifier needed
        pr_min = pm_types.PersonReference([pm_types.InstanceIdentifier('root')])
        for obj in (pr_max, pr_min):
            self.check_convert(obj)

    def test_person_participation(self):
        pp_max = pm_types.PersonParticipation([pm_types.InstanceIdentifier('root'), pm_types.InstanceIdentifier('root2')],
                                             name=pm_types.BaseDemographics('Charles', ['M.'], 'Schulz', 'Meyer', 'Dr.'),
                                             roles=[pm_types.CodedValue('abc')])
        pp_min = pm_types.PersonParticipation()
        for obj in (pp_max, pp_min):
            self.check_convert(obj)

    def test_person_reference_oneof(self):
        pr_max = pm_types.PersonReference([pm_types.InstanceIdentifier('root'), pm_types.InstanceIdentifier('root2')],
                                         name=pm_types.BaseDemographics('Charles', ['M.'], 'Schulz', 'Meyer', 'Dr.'))
        pp_max = pm_types.PersonParticipation([pm_types.InstanceIdentifier('root'), pm_types.InstanceIdentifier('root2')],
                                             name=pm_types.BaseDemographics('Charles', ['M.'], 'Schulz', 'Meyer', 'Dr.'),
                                             roles=[pm_types.CodedValue('abc')])
        for obj in (pr_max, pp_max):
            self.check_convert(obj)

    def test_reference_range(self):
        rr_max = pm_types.ReferenceRange(ref_range=pm_types.Range(Decimal('0.11'), Decimal(42)), meaning=pm_types.CodedValue('42'))
        rr_min = pm_types.ReferenceRange(ref_range=pm_types.Range(Decimal(0), Decimal(1)))
        for obj in (rr_max, rr_min):
            self.check_convert(obj)

    def test_related_measurement(self):
        _rr1 = pm_types.ReferenceRange(ref_range=pm_types.Range(Decimal('0.11'), Decimal(42)), meaning=pm_types.CodedValue('42'))
        _rr2 = pm_types.ReferenceRange(ref_range=pm_types.Range(Decimal(0), Decimal(1)))
        rm_max = pm_types.RelatedMeasurement(
            value=pm_types.Measurement(Decimal(42), pm_types.CodedValue('abc')),
            reference_range=[_rr1, _rr2])
        rm_min = pm_types.RelatedMeasurement(value=pm_types.Measurement(Decimal(1), pm_types.CodedValue('def')))
        for obj in (rm_max, rm_min):
            self.check_convert(obj)

    def test_clinical_info(self):
        related_measurements = [
            pm_types.RelatedMeasurement(pm_types.Measurement(Decimal('42'), pm_types.CodedValue('def'))),
            pm_types.RelatedMeasurement(pm_types.Measurement(Decimal('43'), pm_types.CodedValue('xyz')))]
        # related_measurements = [
        #     pm_types.Measurement(Decimal('3.14'), pm_types.CodedValue('def')),
        #     pm_types.Measurement(Decimal('43'), pm_types.CodedValue('xyz'))]
        ci_max = pm_types.ClinicalInfo(
            type_=pm_types.CodedValue('abc'),
            descriptions=[pm_types.LocalizedText('a', 'de'),
                          pm_types.LocalizedText('b', 'en')],
            related_measurements=related_measurements)
        ci_min = pm_types.ClinicalInfo()
        for obj in (ci_max, ci_min):
            self.check_convert(obj)

    def test_imaging_procedure(self):
        imgp_max = pm_types.ImagingProcedure(accession_identifier=pm_types.InstanceIdentifier('abc'),
                                            requested_procedure_id=pm_types.InstanceIdentifier('abc'),
                                            study_instance_uid=pm_types.InstanceIdentifier('abc'),
                                            scheduled_procedure_step_id=pm_types.InstanceIdentifier('abc'),
                                            modality=pm_types.CodedValue('333'),
                                            protocol_code=pm_types.CodedValue('333'))
        imgp_min = pm_types.ImagingProcedure(accession_identifier=pm_types.InstanceIdentifier('abc'),
                                            requested_procedure_id=pm_types.InstanceIdentifier('abc'),
                                            study_instance_uid=pm_types.InstanceIdentifier('abc'),
                                            scheduled_procedure_step_id=pm_types.InstanceIdentifier('abc'))
        for obj in (imgp_max, imgp_min):
            self.check_convert(obj)

    def test_order_detail(self):
        _imgp_max = pm_types.ImagingProcedure(accession_identifier=pm_types.InstanceIdentifier('abc'),
                                            requested_procedure_id=pm_types.InstanceIdentifier('abc'),
                                            study_instance_uid=pm_types.InstanceIdentifier('abc'),
                                            scheduled_procedure_step_id=pm_types.InstanceIdentifier('abc'),
                                            modality=pm_types.CodedValue('333'),
                                            protocol_code=pm_types.CodedValue('333'))

        od_max = pm_types.OrderDetail(
            start='2020-10-31',
            end='2020-11-01',
            performer=[pm_types.PersonParticipation()],
            service=[pm_types.CodedValue('abc')],
            imaging_procedure=[_imgp_max]
        )
        od_min = pm_types.OrderDetail()
        for obj in (od_max, od_min):
            self.check_convert(obj)

    def test_requested_order_detail(self):
        _imgp_max = pm_types.ImagingProcedure(accession_identifier=pm_types.InstanceIdentifier('abc'),
                                            requested_procedure_id=pm_types.InstanceIdentifier('abc'),
                                            study_instance_uid=pm_types.InstanceIdentifier('abc'),
                                            scheduled_procedure_step_id=pm_types.InstanceIdentifier('abc'),
                                            modality=pm_types.CodedValue('333'),
                                            protocol_code=pm_types.CodedValue('333'))
        pr_max = pm_types.PersonReference([pm_types.InstanceIdentifier('root'), pm_types.InstanceIdentifier('root2')],
                                         name=pm_types.BaseDemographics('Charles', ['M.'], 'Schulz', 'Meyer', 'Dr.'))
        pr_max2 = pm_types.PersonReference([pm_types.InstanceIdentifier('root'),],
                                           name=pm_types.BaseDemographics('Charles', ['M.'], 'Schulz', 'Meyer', 'Dr.'))

        od_max = pm_types.RequestedOrderDetail(
            start='2020-10-31',
            end='2020-11-01',
            performer=[pm_types.PersonParticipation()],
            service=[pm_types.CodedValue('abc')],
            imaging_procedure=[_imgp_max],
            referring_physician=pr_max,
            requesting_physician=pr_max2,
            placer_order_number=pm_types.InstanceIdentifier('root')
        )
        od_min = pm_types.RequestedOrderDetail()
        for obj in (od_max, od_min):
            self.check_convert(obj)


    def test_performed_order_detail(self):
        _imgp_max = pm_types.ImagingProcedure(accession_identifier=pm_types.InstanceIdentifier('abc'),
                                            requested_procedure_id=pm_types.InstanceIdentifier('abc'),
                                            study_instance_uid=pm_types.InstanceIdentifier('abc'),
                                            scheduled_procedure_step_id=pm_types.InstanceIdentifier('abc'),
                                            modality=pm_types.CodedValue('333'),
                                            protocol_code=pm_types.CodedValue('333'))
        od_max = pm_types.PerformedOrderDetail(
            start='2020-10-31',
            end='2020-11-01',
            performer=[pm_types.PersonParticipation()],
            service=[pm_types.CodedValue('abc')],
            imaging_procedure=[_imgp_max],
            filler_order_number=pm_types.InstanceIdentifier('abc'),
            resulting_clinical_info=[pm_types.ClinicalInfo(
                type_=pm_types.CodedValue('abc'),
                descriptions=[pm_types.LocalizedText('a', 'de'),
                              pm_types.LocalizedText('b', 'en')],
                related_measurements=None
            )]
        )
        od_min = pm_types.PerformedOrderDetail()
        for obj in (od_max, od_min):
            self.check_convert(obj)

    def test_workflow_detail(self):
        patient = pm_types.PersonReference([pm_types.InstanceIdentifier('root'), pm_types.InstanceIdentifier('root2')],
                                          name=pm_types.BaseDemographics('Charles', ['M.'], 'Schulz', 'Meyer', 'Dr.'))
        referring_physician = pm_types.PersonReference(
            [pm_types.InstanceIdentifier('root2')],
            name=pm_types.BaseDemographics('Cindy', ['L.'], 'Miller', 'Meyer', 'Dr.'))
        requesting_physician = pm_types.PersonReference(
            [pm_types.InstanceIdentifier('root2')],
            name=pm_types.BaseDemographics('Henry', ['L.'], 'Miller'))

        assigned_location = pm_types.LocationReference(identifications=[pm_types.InstanceIdentifier('root'),
                                                                       pm_types.InstanceIdentifier('root2')],
                                                      location_detail=pm_types.LocationDetail('poc', 'room', 'bed'))
        visit_number = pm_types.InstanceIdentifier('abc')
        danger_codes = [pm_types.CodedValue('434343'), pm_types.CodedValue('545454')]
        relevant_clinical_info = [pm_types.ClinicalInfo(type_=pm_types.CodedValue('abc'),
                                                       descriptions=[pm_types.LocalizedText('a', 'de'),
                                                                     pm_types.LocalizedText('b', 'en')],
                                                       related_measurements=None),
                                  pm_types.ClinicalInfo(type_=pm_types.CodedValue('yxz'),
                                                       descriptions=[pm_types.LocalizedText('u', 'de'),
                                                                     pm_types.LocalizedText('v', 'en')],
                                                       related_measurements=None)]
        requested_order_detail = pm_types.RequestedOrderDetail(
            start='2020-10-31',
            end='2020-11-01',
            performer=[pm_types.PersonParticipation()],
            service=[pm_types.CodedValue('abc')],
            imaging_procedure=[],
            referring_physician=referring_physician,
            requesting_physician=requesting_physician,
            placer_order_number=pm_types.InstanceIdentifier('root'))
        performed_order_detail = pm_types.PerformedOrderDetail(
            start='2020-10-31',
            end='2020-11-01',
            performer=[pm_types.PersonParticipation()],
            service=[pm_types.CodedValue('abc')],
            imaging_procedure=[],
            filler_order_number=pm_types.InstanceIdentifier('abc'),
            resulting_clinical_info=[pm_types.ClinicalInfo(
                type_=pm_types.CodedValue('abc'),
                descriptions=[pm_types.LocalizedText('a', 'de'),
                              pm_types.LocalizedText('b', 'en')],
                related_measurements=None
            )]
)

        wfd_max = pm_types.WorkflowDetail(patient, assigned_location, visit_number, danger_codes,
                                         relevant_clinical_info, requested_order_detail, performed_order_detail)
        wfd_min = pm_types.WorkflowDetail()
        for obj in (wfd_max, wfd_min):
            self.check_convert(obj)

    def test_relation(self):
        rel_max = pm_types.Relation()
        rel_max.Code = pm_types.CodedValue('abc')
        rel_max.Identification = pm_types.InstanceIdentifier('root')
        rel_max.Kind = pm_types.AbstractMetricDescriptorRelationKindEnum.OTHER
        rel_max.Entries = ['a', 'b', 'c']
        rel_min = pm_types.Relation()
        for obj in (rel_max, rel_min):
            self.check_convert(obj)

    @unittest.skip
    def test_selector(self):
        sel_max = pm_types.T_Selector()
        sel_min = pm_types.T_Selector()
        for obj in (sel_max, sel_min):
            obj_p = pmtypesmapper.selector_to_p(obj, None)
            obj2 = pmtypesmapper.selector_from_p(obj_p)
            self.assertEqual(obj.__class__, obj2.__class__)
            delta = diff(obj, obj2)
            self.assertEqual(obj, obj2)

    @unittest.skip
    def test_dual_channel_def(self):
        pass

    @unittest.skip
    def test_safety_context_def(self):
        pass

    @unittest.skip
    def test_safety_req_def(self):
        pass

    def test_udi(self):
        udi_max = pm_types.UdiType(
            device_identifier='prx.y.3',
            human_readable_form='my_device',
            issuer= pm_types.InstanceIdentifier('root'),
            jurisdiction=pm_types.InstanceIdentifier('jurisd')
        )
        udi_min = pm_types.UdiType('', '', pm_types.InstanceIdentifier('root'))
        for obj in (udi_max, udi_min):
            self.check_convert(obj)

    def test_metadata(self):
        m_min = pm_types.MetaData()
        m_max = pm_types.MetaData()
        m_max.Udi = [pm_types.UdiType(device_identifier='prx.y.3',
                                   human_readable_form='my_device',
                                   issuer= pm_types.InstanceIdentifier('root'),
                                   jurisdiction=pm_types.InstanceIdentifier('jurisd') )
                     ]
        m_max.LotNumber = '08/15'
        m_max.Manufacturer = [pm_types.LocalizedText('a', 'de'), pm_types.LocalizedText('b', 'en')]
        m_max.ManufactureDate = '2020_01_01'
        m_max.ExpirationDate = '2020_01_02'
        m_max.ModelName = [pm_types.LocalizedText('foo', 'de'), pm_types.LocalizedText('bar', 'en')]
        m_max.ModelNumber = '4711'
        m_max.SerialNumber = ['abcd-1234']
        for obj in (m_max, m_min):
            self.check_convert(obj)

    def test_operation_group(self):
        og_max = pm_types.OperationGroup()
        og_max.Type = pm_types.CodedValue('abc')
        og_max.OperatingMode = pm_types.OperatingMode.NA
        og_max.Operations = ['a', 'b']
        og_min = pm_types.OperationGroup()
        for obj in (og_max, og_min):
            self.check_convert(obj)

