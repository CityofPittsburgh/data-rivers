from __future__ import absolute_import

import logging
import os

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.avroio import WriteToAvro

from dataflow_utils import dataflow_utils
from dataflow_utils.dataflow_utils import JsonCoder, SwapFieldNames, generate_args, ChangeDataTypes, FilterFields

DEFAULT_DATAFLOW_ARGS = [
    '--save_main_session',
    f"--project={os.environ['GCLOUD_PROJECT']}",
    f"--service_account_email={os.environ['SERVICE_ACCT']}",
    f"--region={os.environ['REGION']}",
    f"--subnetwork={os.environ['SUBNET']}"
]


def run(argv=None):
    # assign the name for the job and specify the AVRO upload location (GCS bucket), arg parser object,
    # and avro schema to validate data with. Return the arg parser values, PipelineOptions, and avro_schemas (dict)

    known_args, pipeline_options, avro_schema = generate_args(
        job_name='ceridian-accruals-dataflow',
        bucket=f"{os.environ['GCS_PREFIX']}_ceridian",
        argv=argv,
        schema_name='timebank_accruals',
        default_arguments=DEFAULT_DATAFLOW_ARGS,
        limit_workers=[False, None]
    )

    with beam.Pipeline(options=pipeline_options) as p:
        field_name_swaps = [('Employee_XRefCode', 'employee_id'),
                            ('Balance_ShortName', 'time_bank'),
                            ('EntitlementBalance_CurrentValue', 'balance')]
        type_changes = [('employee_id', 'str'), ('balance', 'float')]
        drop_fields = ['Employee_NameNumber', 'EmployeeEmploymentStatus_BaseRate',
                       'EntitlementPolicy_LongName', 'DeptJob_LongName']

        lines = p | ReadFromText(known_args.input, coder=JsonCoder())

        load = (
                lines
                | beam.ParDo(SwapFieldNames(field_name_swaps))
                | beam.ParDo(ChangeDataTypes(type_changes))
                | beam.ParDo(FilterFields(drop_fields, exclude_target_fields=True))
                | WriteToAvro(known_args.avro_output, schema=avro_schema, file_name_suffix='.avro',
                              use_fastavro=True)
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()