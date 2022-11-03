from __future__ import absolute_import

import logging
import os

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.avroio import WriteToAvro

from dataflow_utils import dataflow_utils
from dataflow_utils.dataflow_utils import JsonCoder, SwapFieldNames, generate_args, FilterFields,\
    ChangeDataTypes, ExtractField

DEFAULT_DATAFLOW_ARGS = [
        '--save_main_session',
        f"--project={os.environ['GCLOUD_PROJECT']}",
        f"--service_account_email={os.environ['SERVICE_ACCT']}",
        f"--region={os.environ['REGION']}",
        f"--subnetwork={os.environ['SUBNET']}"
]

class ConvertGeography(beam.DoFn):
    def __init__(self, geo_field):
        """
        :param geo_field - Name of field that will be converted into string formatted for BQ geography conversion
        """
        self.geo_field = geo_field
    def process(self, datum):
        coord_list = datum[self.geo_field][datum[self.geo_field].find("[{")+2:datum[self.geo_field].find("}]")].split('}, {')
        formatted_geo = ''
        i = 1
        for coord in coord_list:
            lat_lng = coord.split(', ')
            rev_str = lat_lng[1].split(': ')[1] + " " + lat_lng[0].split(': ')[1]
            if i < len(coord_list):
                formatted_geo += rev_str + ", "
            else:
                formatted_geo += rev_str
            i += 1
        datum[self.geo_field] = 'POLYGON((' + formatted_geo + '))'

        yield datum

def run(argv = None):
    # assign the name for the job and specify the AVRO upload location (GCS bucket), arg parser object,
    # and avro schema to validate data with. Return the arg parser values, PipelineOptions, and avro_schemas (dict)

    known_args, pipeline_options, avro_schema = generate_args(
            job_name = 'cartegraph-facilities-dataflow',
            bucket = f"{os.environ['GCS_PREFIX']}_cartegraph",
            argv = argv,
            schema_name = 'cartegraph_facilities',
            default_arguments = DEFAULT_DATAFLOW_ARGS,
            limit_workers = [False, None]
    )

    with beam.Pipeline(options = pipeline_options) as p:
        source_fields = ['CgShape', 'CgShape', 'CgShape', 'FacilitySizeField']
        nested_fields = ['Points', 'Center', 'Center', 'Amount']
        additional_nested_fields = ['', 'Lat', 'Lng', '']
        new_field_names = ['geometry', 'lat', 'long', 'size_sq_foot']
        field_name_swaps = [('Oid', 'id'), ('IDField', 'name'), ('FacilityTypeField', 'type'),
                            ('DescriptionField', 'description'), ('NotesField', 'notes'),
                            ('PrimaryUserField', 'primary_user'), ('InstalledField', 'installed_date'),
                            ('EntryDateField', 'entry_date'), ('cgLastModifiedField', 'last_modified_date'),
                            ('cgProbabilityOfFailureScoreField', 'probability_of_failure_score'),
                            ('PlannedRenovationsField', 'planned_renovations'),
                            ('PlannedLevelofInterventionField', 'planned_intervention_level'),
                            ('PlannedYearofInterventionField', 'planned_intervention_year'),
                            ('AddressNumberField', 'street_num'), ('StreetField', 'street_name'),
                            ('ZipCodeField', 'zip'), ('ParcelIDField', 'parcel'), ('ParkField', 'park'),
                            ('NeighborhoodField', 'neighborhood_name'), ('CouncilDistrictField', 'council_district'),
                            ('PublicViewField', 'public_view'), ('PublicRestroomsField', 'public_restrooms'),
                            ('RentableField', 'is_rentable'), ('VacantField', 'is_vacant'),
                            ('FloorCountField', 'floor_count'), ('FloorsBelowGradeField', 'floors_below_grade'),
                            ('FoundationTypeField', 'foundation_type'),  ('BuildingEnvelopeField', 'building_envelope'),
                            ('ParkingField', 'parking_type'), ('ADANotesField', 'ada_notes'),
                            ('ADAAccessibleApproachEntranceField', 'ada_accessible_approach_entrance'),
                            ('ADAAccessibletoGoodsandServicesField', 'ada_accessible_to_goods_and_services'),
                            ('ADAAdditionalAccessField', 'ada_additional_access'),
                            ('ADAUsabilityofRestroomsField', 'ada_usability_of_restrooms'),
                            ('ADAAssessmentDateField', 'ada_assessment_date'), ('TotalCostField', 'total_cost'),
                            ('SavingOpportunityField', 'saving_opportunity'),
                            ('EnergyRenovationCostEstimateField', 'energy_renovation_cost_estimate'),
                            ('ReplacedField', 'replaced_date'), ('ReplacementCostTypeField', 'replacement_cost_type')]
        keep_fields = ['id', 'name', 'type', 'description', 'notes', 'primary_user', 'installed_date',
                       'entry_date', 'last_modified_date', 'probability_of_failure_score', 'planned_renovations',
                       'planned_intervention_level', 'planned_intervention_year', 'street_num', 'street_name',
                       'zip', 'park', 'neighborhood_name', 'council_district', 'public_view', 'public_restrooms',
                       'is_rentable', 'is_vacant', 'floor_count', 'floors_below_grade', 'foundation_type',
                       'building_envelope', 'parking_type', 'ada_notes', 'ada_accessible_approach_entrance',
                       'ada_accessible_to_goods_and_services', 'ada_additional_access', 'ada_usability_of_restrooms',
                       'ada_assessment_date', 'total_cost', 'saving_opportunity', 'energy_renovation_cost_estimate',
                       'replaced_date', 'replacement_cost_type', 'size_sq_foot', 'lat', 'long', 'geometry']
        type_changes = [('id', 'str'), ('probability_of_failure_score', 'str'), ('planned_intervention_level', 'str'),
                        ('planned_intervention_year', 'str'), ('street_num', 'str'), ('zip', 'str'),
                        ('council_district', 'str'), ('public_view', 'bool'), ('public_restrooms', 'bool'),
                        ('is_rentable', 'bool'), ('is_vacant', 'bool'), ('floor_count', 'int'),
                        ('floors_below_grade', 'int'), ('total_cost', 'float'), ('saving_opportunity', 'float'),
                        ('energy_renovation_cost_estimate', 'float'), ('size_sq_foot', 'float')]

        lines = p | ReadFromText(known_args.input, coder = JsonCoder())

        load = (
                lines
                | beam.ParDo(ExtractField(source_fields, nested_fields, new_field_names, additional_nested_fields))
                | beam.ParDo(SwapFieldNames(field_name_swaps))
                | beam.ParDo(FilterFields(keep_fields, exclude_target_fields=False))
                | beam.ParDo(ChangeDataTypes(type_changes))
                | beam.ParDo(ConvertGeography('geometry'))
                | WriteToAvro(known_args.avro_output, schema = avro_schema, file_name_suffix = '.avro',
                              use_fastavro = True)
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()