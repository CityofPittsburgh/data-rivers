from __future__ import absolute_import

import logging
import os

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.avroio import WriteToAvro

from dataflow_utils import dataflow_utils
from dataflow_utils.dataflow_utils import generate_args, JsonCoder, ColumnsCamelToSnakeCase


def run(argv=None):

    known_args, pipeline_options, avro_schema = dataflow_utils.generate_args(
        job_name='comm-ctr-attendance-dataflow',
        bucket='{}_community_centers'.format(os.environ['GCS_PREFIX']),
        argv=argv,
        schema_name='community_center_attendance',
        runner='DataflowRunner'
    )

    with beam.Pipeline(options=pipeline_options) as p:
        # Read the text file[pattern] into a PCollection.
        lines = p | ReadFromText(known_args.input, coder=dataflow_utils.JsonCoder())

        load = (
                lines
                | beam.ParDo(ColumnsCamelToSnakeCase())
                | WriteToAvro(known_args.avro_output, schema=avro_schema, file_name_suffix='.avro', use_fastavro=True))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
