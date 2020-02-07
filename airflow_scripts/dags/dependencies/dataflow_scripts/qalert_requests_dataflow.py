from __future__ import absolute_import

import argparse
import json
import logging
import os

import apache_beam as beam
import avro
import fastavro
import dataflow_utils

from apache_beam.io import ReadFromText
from apache_beam.io.avroio import WriteToAvro
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from avro import schema
from datetime import datetime

from dataflow_utils import generate_args, get_schema, JsonCoder

def run(argv=None):
    dt = datetime.now()
    parser = argparse.ArgumentParser()

    parser.add_argument('--input',
                        dest='input',
                        default='gs://{}_311/{}/{}/{}_requests.json'.format(os.environ['GCS_PREFIX'],
                                                                                                dt.strftime('%Y'),
                                                                                                dt.strftime('%m').lower(),
                                                                                                dt.strftime("%Y-%m-%d")),
                        help='Input file to process.')
    parser.add_argument('--avro_output',
                        dest='avro_output',
                        default='gs://{}_311/avro_output/{}/{}/{}/avro_output'.format(os.environ['GCS_PREFIX'],
                                                                                             dt.strftime('%Y'),
                                                                                             dt.strftime('%m').lower(),
                                                                                             dt.strftime("%Y-%m-%d")),
                        help='Output directory to write avro files.')

    known_args, pipeline_args = parser.parse_known_args(argv)

    #TODO: run on on-prem network when route is opened
    # Use runner=DataflowRunner to run in GCP environment, DirectRunner to run locally
    pipeline_args.extend(generate_args('qalert-requests-dataflow',
                                       '{}_311'.format(os.environ['GCS_PREFIX']),
                                       'DirectRunner'))

    avro_schema = get_schema('City_of_Pittsburgh_QAlert_Requests')

    pipeline_options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=pipeline_options) as p:
        # Read the text file[pattern] into a PCollection.
        lines = p | ReadFromText(known_args.input, coder=JsonCoder())

        load = (
                lines
                | WriteToAvro(known_args.avro_output, schema=avro_schema, file_name_suffix='.avro', use_fastavro=True))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()