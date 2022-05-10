from __future__ import absolute_import

import argparse
import logging
import os
from datetime import datetime

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.avroio import WriteToAvro
from apache_beam.options.pipeline_options import PipelineOptions

from dataflow_utils import dataflow_utils
from dataflow_utils.dataflow_utils import get_schema, generate_args, JsonCoder


def run(argv=None):

    known_args, pipeline_options, avro_schema = generate_args(
        job_name='otrs-tickets-dataflow',
        bucket='{}_otrs'.format(os.environ['GCS_PREFIX']),
        argv=argv,
        schema_name='City_of_Pittsburgh_OTRS_Ticket'
    )

    with beam.Pipeline(options=pipeline_options) as p:
        # Read the text file[pattern] into a PCollection.
        lines = p | ReadFromText(known_args.input, coder = JsonCoder())

        load = (
                lines
                | WriteToAvro(known_args.avro_output, schema=avro_schema, file_name_suffix='.avro', use_fastavro=True))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
