from __future__ import absolute_import

import logging
import os

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.avroio import WriteToAvro

from dataflow_utils import dataflow_utils
from dataflow_utils.dataflow_utils import JsonCoder, SwapFieldNames, generate_args, FilterFields, \
    ColumnsCamelToSnakeCase, GetDateStringsFromUnix, ChangeDataTypes, AnonymizeLatLong

DEFAULT_DATAFLOW_ARGS = [
        '--save_main_session',
        f"--project={os.environ['GCLOUD_PROJECT']}",
        f"--service_account_email={os.environ['SERVICE_ACCT']}",
        f"--region={os.environ['REGION']}",
        f"--subnetwork={os.environ['SUBNET']}"
]

class DetectChildTicketStatus(beam.DoFn):
    def process(self, datum):
        if datum['parent_ticket_id'] == "0":
            datum['child_ticket'] = False
        else:
            datum['child_ticket'] = True
        yield datum


def run(argv = None):
    # assign the name for the job and specify the AVRO upload location (GCS bucket), arg parser object,
    # and avro schema to validate data with. Return the arg parser values, PipelineOptions, and avro_schemas (dict)

    known_args, pipeline_options, avro_schema = generate_args(
            job_name = 'qalert-lat-long-backfill-dataflow',
            bucket = f"{os.environ['GCS_PREFIX']}_qalert",
            argv = argv,
            schema_name = 'qalert_lat_long_backfill',
            default_arguments = DEFAULT_DATAFLOW_ARGS,
            limit_workers = [False, None]
    )

    with beam.Pipeline(options = pipeline_options) as p:
        field_name_swaps = [("master", "parent_ticket_id"),
                            ("latitude", "input_pii_lat"),
                            ("longitude", "input_pii_long")]

        type_changes = [("id", "str"), ("parent_ticket_id", "str")]

        lat_long_accuracy = [("input_pii_lat", "input_pii_long", 200)]

        lines = p | ReadFromText(known_args.input, coder = JsonCoder())

        load = (
                lines
                | beam.ParDo(SwapFieldNames(field_name_swaps))
                | beam.ParDo(ChangeDataTypes(type_changes))
                | beam.ParDo(DetectChildTicketStatus())
                | beam.ParDo(AnonymizeLatLong(lat_long_accuracy))
                | WriteToAvro(known_args.avro_output, schema = avro_schema, file_name_suffix = '.avro',
                              use_fastavro = True)
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
