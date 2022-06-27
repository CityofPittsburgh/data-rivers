from __future__ import absolute_import

import logging
import os

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.avroio import WriteToAvro

# import util modules.
# util modules located one level down in directory (./dataflow_util_modules/datflow_utils.py)
from dataflow_utils.dataflow_utils import JsonCoder, ConvertBooleans, StandardizeTimes, \
    FilterFields, ConvertStringCase, generate_args


def run(argv = None):
    known_args, pipeline_options, avro_schema = generate_args(
            job_name = 'computronix-domi-street-closures-gis',
            bucket = '{}_computronix'.format(os.environ['GCS_PREFIX']),
            argv = argv,
            schema_name = 'computronix_gis_street_closures'
    )

    with beam.Pipeline(options = pipeline_options) as p:
        times = [("create_date", "EST"), ("from_date", "EST"), ("to_date", "EST")]

        bool_convs = [("full_closure", "Y", "N", False), ("travel_lane", "Y", "N", False),
                      ("parking_lane", "Y", "N", False), ("metered_parking", "Y", "N", False),
                      ("sidewalk", "Y", "N", False), ("validated", "Y", "N", False)]

        str_convs = [("ext_file_num", "upper"), ("permit_type", "title"), ("work_desc", "sentence"),
                     ("type_work_desc", "title"), ("contractor_name", "title"), ("special_instructions", "sentence"),
                     ("weekday_hours", "upper"), ("weekend_hours", "upper"), ("primary_street", "upper"),
                     ("from_street", "upper"), ("to_street", "upper")]

        drops = ["create_date", "from_date", "to_date", "street_segment", "street_closure"]

        lines = p | ReadFromText(known_args.input, coder = JsonCoder())

        load = (
                lines
                | beam.ParDo(ConvertBooleans(bool_convs, True))
                | beam.ParDo(ConvertStringCase(str_convs))
                | beam.ParDo(StandardizeTimes(times))
                | beam.ParDo(FilterFields(drops))
                | WriteToAvro(known_args.avro_output, schema = avro_schema, file_name_suffix = '.avro',
                              use_fastavro = True))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
