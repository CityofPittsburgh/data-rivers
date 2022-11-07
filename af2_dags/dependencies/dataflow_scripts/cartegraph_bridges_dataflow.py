from __future__ import absolute_import

import logging
import os

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.avroio import WriteToAvro

from dataflow_utils import dataflow_utils
from dataflow_utils.dataflow_utils import JsonCoder, SwapFieldNames, ColumnsCamelToSnakeCase, generate_args, FilterFields,\
    ChangeDataTypes, ExtractField, ConvertGeography

DEFAULT_DATAFLOW_ARGS = [
        '--save_main_session',
        f"--project={os.environ['GCLOUD_PROJECT']}",
        f"--service_account_email={os.environ['SERVICE_ACCT']}",
        f"--region={os.environ['REGION']}",
        f"--subnetwork={os.environ['SUBNET']}"
]

def run(argv = None):
    # assign the name for the job and specify the AVRO upload location (GCS bucket), arg parser object,
    # and avro schema to validate data with. Return the arg parser values, PipelineOptions, and avro_schemas (dict)

    known_args, pipeline_options, avro_schema = generate_args(
            job_name = 'cartegraph-bridges-dataflow',
            bucket = f"{os.environ['GCS_PREFIX']}_cartegraph",
            argv = argv,
            schema_name = 'cartegraph_bridges',
            default_arguments = DEFAULT_DATAFLOW_ARGS,
            limit_workers = [False, None]
    )

    with beam.Pipeline(options = pipeline_options) as p:
        source_fields = ['CgShape', 'CgShape', 'CgShape', 'LengthField', 'WidthField', 'HeightField',
                         'WeightLimitField', 'SidewalkWidthLeftField', 'SidewalkWidthRightField',
                         'EstimatedTreatmentTimeField']
        nested_fields = ['Points', 'Center', 'Center', 'Amount', 'Amount', 'Amount', 'Amount',
                         'Amount', 'Amount', 'Amount']
        additional_nested_fields = ['', 'Lat', 'Lng', '', '', '', '', '', '', '']
        new_field_names = ['geometry', 'lat', 'long', 'length_ft', 'width_ft', 'height_ft', 'weight_limit_tons',
                           'left_sidewalk_width_ft', 'right_sidewalk_width_ft', 'estimated_treatment_time_min']
        field_name_swaps = [('id', 'name'), ('oid', 'id'), ('replaced', 'replaced_date'),
                            ('cg_last_modified', 'last_modified_date'), ('retired', 'retired_date'),
                            ('culvert_condition_ratings', 'culvert_condition_rating'),
                            ('numberof_spans', 'number_of_spans'), ('drainage', 'has_drainage'),
                            ('neighborhood', 'neighborhood_name'), ('neighborhood2', 'neighborhood_name_2'),
                            ('council_district2', 'council_district_2')]
        keep_fields = ['id', 'name', 'average_daily_traffic', 'notes', 'owner', 'maintenance_responsibility',
                       'entry_date', 'replaced_date', 'last_modified_date', 'retired_date', 'length_ft', 'width_ft',
                       'height_ft', 'weight_limit_tons', 'left_sidewalk_width_ft', 'right_sidewalk_width_ft',
                       'use_type', 'travel_lanes', 'deck_type', 'deck_wearing_surface', 'deck_condition_rating',
                       'structure_type', 'substructure_condition_rating', 'superstructure_condition_rating',
                       'culvert_condition_rating', 'paint_type', 'number_of_spans', 'scuppers_with_downspouts',
                       'scuppers_without_downspouts', 'has_drainage', 'walkway_maintenance_priority', 'total_cost',
                       'estimated_treatment_time_min', 'feature_crossed', 'crosses', 'street_name', 'park',
                       'neighborhood_name', 'neighborhood_name_2', 'council_district', 'council_district_2',
                       'lat', 'long', 'geometry']
        type_changes = [('id', 'str'), ('length_ft', 'float'), ('width_ft', 'float'), ('height_ft', 'float'),
                        ('weight_limit_tons', 'float'), ('left_sidewalk_width_ft', 'float'),
                        ('right_sidewalk_width_ft', 'float'), ('travel_lanes', 'int'), ('number_of_spans', 'int'),
                        ('scuppers_with_downspouts', 'int'), ('scuppers_without_downspouts', 'int'),
                        ('has_drainage', 'bool'), ('total_cost', 'float'), ('estimated_treatment_time_min', 'float'),
                        ('council_district', 'str'), ('council_district_2', 'str')]

        lines = p | ReadFromText(known_args.input, coder = JsonCoder())

        load = (
                lines
                | beam.ParDo(ExtractField(source_fields, nested_fields, new_field_names, additional_nested_fields))
                | beam.ParDo(ColumnsCamelToSnakeCase('Field'))
                | beam.ParDo(SwapFieldNames(field_name_swaps))
                | beam.ParDo(FilterFields(keep_fields, exclude_target_fields=False))
                | beam.ParDo(ChangeDataTypes(type_changes))
                | beam.ParDo(ConvertGeography('geometry', 'LINESTRING'))
                | WriteToAvro(known_args.avro_output, schema = avro_schema, file_name_suffix = '.avro',
                              use_fastavro = True)
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()