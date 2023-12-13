from __future__ import absolute_import

import logging
import os
import json

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.avroio import WriteToAvro

from dataflow_utils import dataflow_utils
from dataflow_utils.dataflow_utils import JsonCoder, SwapFieldNames, generate_args, FilterFields, \
    ColumnsCamelToSnakeCase, ChangeDataTypes, PrependCharacters, GetValsFromExternalFile

DEFAULT_DATAFLOW_ARGS = [
    '--save_main_session',
    f"--project={os.environ['GCLOUD_PROJECT']}",
    f"--service_account_email={os.environ['SERVICE_ACCT']}",
    f"--region={os.environ['REGION']}",
    f"--subnetwork={os.environ['SUBNET']}"
]

# userAccountControl Value Explanations:
# 512                   Enabled Account – Normal Account
# 514                   Disabled Account – Normal Account
# 544                   Enabled Account, Created by Third Party Application (Eg: IDM)
# 4194818, 4194816      Admin Account
# 66048                 Enabled, Password Doesn’t Expire (used in our system for phones, not assigned to users)
class AccountCodeConversion(beam.DoFn):
    def process(self, datum):
        datum['enabled'] = None
        try:
            if datum['userAccountControl'] in ['512', '4194818', '4194816', '544']:
                datum['enabled'] = True
            else:
                datum['enabled'] = False
        except Exception as e:
            print(e)
        yield datum


def run(argv=None):
    # assign the name for the job and specify the AVRO upload location (GCS bucket), arg parser object,
    # and avro schema to validate data with. Return the arg parser values, PipelineOptions, and avro_schemas (dict)

    known_args, pipeline_options, avro_schema = generate_args(
        job_name='active-directory-dataflow',
        bucket=f"{os.environ['GCS_PREFIX']}_active_directory",
        argv=argv,
        schema_name='active_directory_users',
        default_arguments=DEFAULT_DATAFLOW_ARGS,
        limit_workers=[False, None]
    )

    with beam.Pipeline(options=pipeline_options) as p:
        field_name_swaps = [('givenName', 'first_name'), ('sn', 'last_name'),
                            ('sAMAccountName', 'sam_account_name'), ('mail', 'email')]
        dept_name_params = ['ad_department_mapping.json', 'department', 'department']
        type_changes = [('employee_id', 'str')]
        drop_fields = ['userAccountControl']

        lines = p | ReadFromText(known_args.input, coder=JsonCoder())

        load = (
                lines
                | beam.ParDo(AccountCodeConversion())
                | beam.ParDo(GetValsFromExternalFile(dept_name_params[0], dept_name_params[1], dept_name_params[2]))
                | beam.ParDo(SwapFieldNames(field_name_swaps))
                | beam.ParDo(ColumnsCamelToSnakeCase())
                | beam.ParDo(ChangeDataTypes(type_changes))
                | beam.ParDo(PrependCharacters('employee_id', 6, char='0', check_numeric=True))
                | beam.ParDo(FilterFields(drop_fields))
                | WriteToAvro(known_args.avro_output, schema=avro_schema, file_name_suffix='.avro',
                              use_fastavro=True)
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
