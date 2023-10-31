from __future__ import absolute_import

import logging
import os

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.avroio import WriteToAvro

from dataflow_utils import dataflow_utils
from dataflow_utils.dataflow_utils import JsonCoder, SwapFieldNames, generate_args, FilterFields, ChangeDataTypes

DEFAULT_DATAFLOW_ARGS = [
    '--save_main_session',
    f"--project={os.environ['GCLOUD_PROJECT']}",
    f"--service_account_email={os.environ['SERVICE_ACCT']}",
    f"--region={os.environ['REGION']}",
    f"--subnetwork={os.environ['SUBNET']}"
]

USER_DEFINED_CONST_BUCKET = "user_defined_data"


def run(argv=None):
    # assign the name for the job and specify the AVRO upload location (GCS bucket), arg parser object,
    # and avro schema to validate data with. Return the arg parser values, PipelineOptions, and avro_schemas (dict)

    known_args, pipeline_options, avro_schema = generate_args(
        job_name='qalert-submitters-dataflow',
        bucket=f"{os.environ['GCS_PREFIX']}_qalert",
        argv=argv,
        schema_name='qalert_submitters',
        default_arguments=DEFAULT_DATAFLOW_ARGS,
        limit_workers=[False, None],
        backfill_dag=False
    )

    with beam.Pipeline(options=pipeline_options) as p:
        keep_fields = ['id', 'submitter', 'firstName', 'lastName', 'address', 'address2', 'city',
                       'state', 'zip', 'email', 'phone', 'twitterScreenName', 'lastRequest', 'lastModified',
                       'totalClosed', 'totalRequests', 'text']
        
        field_name_swaps = [('id', 'request_id'),
                            ('submitter', 'submitter_id'),
                            ('firstName', 'first_name'),
                            ('lastName', 'last_name'),
                            ('address2', 'address_2'),
                            ('twitterScreenName', 'twitter_name'),
                            ('lastRequest', 'last_request_date'),
                            ('lastModified', 'last_modified_date'),
                            ('totalClosed', 'curr_total_requests_closed'),
                            ('totalRequests', 'curr_total_requests_made'),
                            ('text', 'satisfaction_level')]

        type_changes = [("request_id", "str"), ("submitter_id", "str"), ("first_name", "nullstr"),
                        ("last_name", "nullstr"), ("address", "nullstr"), ("address_2", "nullstr"), ("city", "nullstr"),
                        ("state", "nullstr"), ("zip", "nullstr"), ("email", "nullstr"), ("phone", "nullstr"),
                        ("twitter_name", "nullstr")]

        lines = p | ReadFromText(known_args.input, coder=JsonCoder())

        load = (
                lines
                | beam.ParDo(FilterFields(keep_fields, exclude_target_fields=False))
                | beam.ParDo(SwapFieldNames(field_name_swaps))
                | beam.ParDo(ChangeDataTypes(type_changes))
                | WriteToAvro(known_args.avro_output, schema=avro_schema, file_name_suffix='.avro',
                              use_fastavro=True)
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
