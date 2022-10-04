from __future__ import absolute_import

import logging
import os

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.avroio import WriteToAvro

from dataflow_utils import dataflow_utils
from dataflow_utils.dataflow_utils import JsonCoder, SwapFieldNames, generate_args, FilterFields, \
    ColumnsCamelToSnakeCase, ChangeDataTypes, ExtractField

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
            job_name = 'intime-dataflow',
            bucket = f"{os.environ['GCS_PREFIX']}_intime",
            argv = argv,
            schema_name = 'intime_employees',
            default_arguments = DEFAULT_DATAFLOW_ARGS,
            limit_workers = [False, None]
    )

    with beam.Pipeline(options = pipeline_options) as p:
        source_fields = ['contacts', 'units', 'units', 'ranks', 'ranks']
        nested_fields = ['infos', 'name', 'validFrom', 'rankName', 'validFrom']
        new_field_names = ['email', 'unit', 'unit_valid_date', 'rank', 'rank_valid_date']
        additional_nested_fields = ['info', '', '', '', '']
        search_fields = [{'type': 'EMAIL'}, 'validTo', 'validTo', 'validTo', 'validTo']
        additional_search_vals = ['pittsburghpa.gov', '', '', '', '']
        field_name_swaps = [('external_id', 'mpoetc_number')]
        type_changes = [('employee_id', 'str')]
        keep_fields = ['employee_id', 'mpoetc_number', 'first_name', 'last_name',
                       'email', 'rank', 'rank_valid_date', 'unit', 'unit_valid_date']

        lines = p | ReadFromText(known_args.input, coder = JsonCoder())

        load = (
                lines
                | beam.ParDo(ExtractField(source_fields, nested_fields, new_field_names,
                                          additional_nested_fields, search_fields, additional_search_vals))
                | beam.ParDo(ColumnsCamelToSnakeCase())
                | beam.ParDo(SwapFieldNames(field_name_swaps))
                | beam.ParDo(ChangeDataTypes(type_changes))
                | beam.ParDo(FilterFields(keep_fields, exclude_target_fields=False))
                | WriteToAvro(known_args.avro_output, schema = avro_schema, file_name_suffix = '.avro',
                              use_fastavro = True)
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
