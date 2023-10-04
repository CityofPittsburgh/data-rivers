from __future__ import absolute_import

import logging
import os

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.avroio import WriteToAvro

from dataflow_utils import dataflow_utils
from dataflow_utils.dataflow_utils import JsonCoder, SwapFieldNames, generate_args, FilterFields, \
    StandardizeTimes, ChangeDataTypes, ExtractField, ColumnsCamelToSnakeCase

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
            job_name = 'cartegraph-tasks-dataflow',
            bucket = f"{os.environ['GCS_PREFIX']}_cartegraph",
            argv = argv,
            schema_name = 'cartegraph_tasks',
            default_arguments = DEFAULT_DATAFLOW_ARGS,
            limit_workers = [False, None]
    )

    with beam.Pipeline(options = pipeline_options) as p:
        source_fields = ['CgShape', 'CgShape']
        nested_fields = ['Center', 'Center']
        additional_nested_fields = ['Lat', 'Lng']
        new_field_names = ['lat', 'long']
        field_name_swaps = [('oid', 'id'), ('start_date_actual', 'actual_start_date'),
                            ('stop_date_actual', 'actual_stop_date'), ('labor_cost_actual', 'labor_cost'),
                            ('equipment_cost_actual', 'equipment_cost'), ('material_cost_actual', 'material_cost'),
                            ('labor_hours_actual', 'labor_hours'), ('cg_asset_id', 'asset_id'),
                            ('cg_asset_type', 'asset_type'), ('notes', 'task_notes')]
        drop_fields = ['CgShape']
        times = [('entry_date', 'US/Eastern'), ('actual_start_date', 'US/Eastern'), ('actual_stop_date', 'US/Eastern')]
        type_changes = [('id', 'str'), ('labor_cost', 'float'), ('equipment_cost', 'float'), ('material_cost', 'float'),
                        ('labor_hours', 'float'), ('actual_start_date_UNIX', 'posint'),
                        ('actual_stop_date_UNIX', 'posint'), ('entry_date_UNIX', 'posint')]

        lines = p | ReadFromText(known_args.input, coder = JsonCoder())

        load = (
                lines
                | beam.ParDo(ExtractField(source_fields, nested_fields, new_field_names, additional_nested_fields))
                | beam.ParDo(ColumnsCamelToSnakeCase('Field'))
                | beam.ParDo(SwapFieldNames(field_name_swaps))
                | beam.ParDo(FilterFields(drop_fields, exclude_target_fields=True))
                | beam.ParDo(StandardizeTimes(times, "%Y-%m-%d %H:%M:%S%z"))
                | beam.ParDo(ChangeDataTypes(type_changes))
                | WriteToAvro(known_args.avro_output, schema = avro_schema, file_name_suffix = '.avro',
                              use_fastavro = True)
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
