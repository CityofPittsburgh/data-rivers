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
            job_name = 'cartegraph-playground-equipment-dataflow',
            bucket = f"{os.environ['GCS_PREFIX']}_cartegraph",
            argv = argv,
            schema_name = 'cartegraph_playground_equipment',
            default_arguments = DEFAULT_DATAFLOW_ARGS,
            limit_workers = [False, None]
    )

    with beam.Pipeline(options = pipeline_options) as p:
        source_fields = ['CgShape', 'CgShape']
        nested_fields = ['Center', 'Center']
        additional_nested_fields = ['Lat', 'Lng']
        new_field_names = ['lat', 'long']
        field_name_swaps = [('id', 'id_num'), ('oid', 'id'), ('equipment_type', 'type'),
                            ('equipment_description', 'description'), ('locator_address_number', 'address_num'),
                            ('locator_street', 'street_name'), ('installed', 'installed_date'),
                            ('cg_last_modified', 'last_modified_date'), ('replaced', 'replaced_date'),
                            ('retired', 'retired_date')]
        keep_fields = ['id', 'type', 'manufacturer', 'description', 'notes', 'playground', 'primary_attachment',
                       'ada_accessible', 'address_num', 'street_name', 'city', 'safety_surface_type', 'entry_date',
                       'installed_date', 'last_modified_date', 'replaced_date', 'retired_date', 'inactive',
                       'total_cost', 'lat', 'long']
        type_changes = [('id', 'str'), ('primary_attachment', 'str'), ('ada_accessible', 'bool'),
                        ('inactive', 'bool'), ('address_num', 'str'), ('total_cost', 'float')]

        lines = p | ReadFromText(known_args.input, coder = JsonCoder())

        load = (
                lines
                | beam.ParDo(ExtractField(source_fields, nested_fields, new_field_names, additional_nested_fields))
                | beam.ParDo(ColumnsCamelToSnakeCase('Field'))
                | beam.ParDo(SwapFieldNames(field_name_swaps))
                | beam.ParDo(FilterFields(keep_fields, exclude_target_fields=False))
                | beam.ParDo(ChangeDataTypes(type_changes))
                | WriteToAvro(known_args.avro_output, schema = avro_schema, file_name_suffix = '.avro',
                              use_fastavro = True)
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()