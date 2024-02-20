from __future__ import absolute_import

import logging
import os
import time
import datetime
from abc import ABC

import apache_beam as beam
from apache_beam.io import ReadFromText, WriteToBigQuery

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


def conv_avsc_to_bq_table_schema(schema):
    """
    Data can be loaded into BQ directly from a BEAM pipeline if a corresponding schema is provided. The format for
    this schema is close, but not identical, to that use by Airflow in a GCSToBQ operation (a typical AVSC). This
    function converts a schema specified in an AVSC file to a BQ table schema. This function allows usage of a single
    schema definition for either BQ upload method.

    :param schema: list of dicts, each dict containing a field and its data type. all other info is removed for
    compatibility with BEAM/BQ formatting. NULL cannot be specified in the resulting output and some data types use
    different naming conventions (e.g. 'int' vs 'int64')
    :return: schema_str - a single string containing all field names and data types, formatted as a big query table
    schema
    """
    schema_str = ""
    change_vals = {"float": "float64", "int": "int64"}
    change_keys = change_vals.keys()
    for s in schema:
        print(s)
        val_type = s["type"]
        name = s["name"]
        if type(val_type) == list and 'null' in val_type:
            val_type.remove('null')
            type_str = val_type[0]
        else:
            type_str = val_type[0]
        if type_str in change_keys:
            type_str_clean = change_vals[type_str]
        else:
            type_str_clean = type_str

        schema_str = schema_str + F"{name}:{type_str_clean},"

    schema_str = schema_str.upper()
    schema_str = schema_str[:-1]

    print(schema_str)

    return schema_str


class IdentifyActivePermits(beam.DoFn, ABC):
    def process(self, datum):
        now = datetime.date.today()
        unix_date = int(time.mktime(now.timetuple()))
        start = datum['from_date_UNIX']
        stop = datum['to_date_UNIX']
        try:
            if start <= unix_date <= stop:
                datum["active"] = 1
            else:
                datum["active"] = 0
        except TypeError:
            datum["active"] = None
        yield datum


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

    schema_str_conv = conv_avsc_to_bq_table_schema(schema = avro_schema['fields'])

    with beam.Pipeline(options = pipeline_options) as p:
        times = [("from_date", "US/Eastern"), ("to_date", "US/Eastern")]

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
                | beam.ParDo(IdentifyActivePermits())
                | WriteToBigQuery(F"{os.environ['GCLOUD_PROJECT']}:computronix.gis_street_closures",
                                  schema = schema_str_conv,
                                  create_disposition = beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                  write_disposition = beam.io.BigQueryDisposition.WRITE_TRUNCATE)

        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
