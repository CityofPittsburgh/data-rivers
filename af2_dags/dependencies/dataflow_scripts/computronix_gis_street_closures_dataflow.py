from __future__ import absolute_import

import logging
import os

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.avroio import WriteToAvro

# import util modules.
# util modules located one level down in directory (./dataflow_util_modules/datflow_utils.py)
from dataflow_utils.dataflow_utils import JsonCoder, ConvertBooleans, StandardizeTimes, \
    FilterOutliers, FilterFields, ConvertStringCase, generate_args


DEFAULT_DATAFLOW_ARGS = [
        '--save_main_session',
        f"--project={os.environ['GCLOUD_PROJECT']}",
        f"--service_account_email={os.environ['SERVICE_ACCT']}",
        f"--region={os.environ['REGION']}",
        f"--subnetwork={os.environ['SUBNET']}"
]


# run function is called at the bottom of the script and the entire operation is defined within
# generate_args will initialize all options/args needed to execute the pipeline. known_args contains the runtime
# params passed in from DAG (input/output). pipeline_options contains all the flags that are initialized by default (
# project/service_acct/etc.). The schema is loaded as a dict
def run(argv = None):
    known_args, pipeline_options, avro_schema = generate_args(
            job_name = 'computronix-domi-street-closures-gis',
            bucket = '{}_computronix'.format(os.environ['GCS_PREFIX']),
            argv = argv,
            schema_name = 'computronix_gis_street_closures',
            default_arguments = DEFAULT_DATAFLOW_ARGS
    )

    with beam.Pipeline(options = pipeline_options) as p:
        times = [("from_date", "EST"), ("to_date", "EST")]

        bool_convs = [("full_closure", "Y", "N", False), ("travel_lane", "Y", "N", False),
                      ("parking_lane", "Y", "N", False), ("metered_parking", "Y", "N", False),
                      ("sidewalk", "Y", "N", False)]

        str_convs = [("ext_file_num", "upper"), ("permit_type", "upper"), ("work_desc", "upper"),
                     ("type_work_desc", "upper"), ("applicant_name", "upper"), ("contractor_name", "upper"),
                     ("special_instructions", "upper"), ("weekday_hours", "upper"), ("weekend_hours", "upper"),
                     ("primary_street", "upper"), ("from_street", "upper"), ("to_street", "upper")]

        drops = ["from_date", "to_date", "street_segment", "street_closure"]

        # only allow times between 1990 and 2050. This can be reconfigured and is simply here to permit the AVRO
        # creation. The goal is for end users to identify outlier dates via UTC/EST and handle those correctly
        outlier_checks = [("from_date_UNIX", 631152000, 2524608000), ("to_date_UNIX", 631152000, 2524608000)]

        lines = p | ReadFromText(known_args.input, coder = JsonCoder())

        load = (
                lines
                | beam.ParDo(ConvertBooleans(bool_convs, True))
                | beam.ParDo(ConvertStringCase(str_convs))
                | beam.ParDo(StandardizeTimes(times, t_format = "%m/%d/%Y %H:%M:%S"))
                | beam.ParDo(FilterOutliers(outlier_checks))
                | beam.ParDo(FilterFields(drops))
                | WriteToAvro(known_args.avro_output, schema = avro_schema, file_name_suffix = '.avro',
                              use_fastavro = True))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
