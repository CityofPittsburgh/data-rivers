from __future__ import absolute_import

import argparse
import json
import logging
import os

import apache_beam as beam
import avro
import fastavro

from apache_beam.io import ReadFromText
from apache_beam.io.avroio import WriteToAvro
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from avro import schema

from datetime import datetime
from google.cloud import storage
from dataflow_utils import dataflow_utils
from dataflow_utils.dataflow_utils import hash_func, download_schema, generate_args, JsonCoder


class Formatter(beam.DoFn):
    def process(self, datum):
        datum['NAICSCODE'] = int(datum['NAICSCODE'])
        yield datum


def run(argv=None):
    dt = datetime.now()
    parser = argparse.ArgumentParser()

    parser.add_argument('--input',
                        dest='input',
                        default='gs://pghpa_computronix/contractors/{}/{}/{}_contractors_licenses.json'.format(dt.strftime('%Y'),
                                                                                          dt.strftime('%m').lower(),
                                                                                          dt.strftime("%Y-%m-%d")),
                        help='Input file to process.')
    parser.add_argument('--avro_output',
                        dest='avro_output',
                        default='gs://pghpa_computronix/contractors/avro_output/{}/{}/{}/avro_output'.format(dt.strftime('%Y'),
                                                                                         dt.strftime('%m').lower(),
                                                                                         dt.strftime("%Y-%m-%d")),
                        help='Output directory to write avro files.')

    known_args, pipeline_args = parser.parse_known_args(argv)

    #TODO: run on on-prem network when route is opened
    # Use runner=DataflowRunner to run in GCP environment, DirectRunner to run locally
    pipeline_args.extend(generate_args('computronix-trades-dataflow', 'pghpa_computronix', 'DirectRunner'))

    schema.RecordSchema.__hash__ = hash_func

    download_schema('pghpa_avro_schemas', 'contractors_computronix.avsc', 'contractors_computronix.avsc')

    SCHEMA_PATH = 'contractors_computronix.avsc'
    # fastavro does the work of avro.schema.parse(), just need to pass dict
    # avro_schema = avro.schema.parse(open(SCHEMA_PATH).read())
    avro_schema = json.loads(open(SCHEMA_PATH).read())

    pipeline_options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=pipeline_options) as p:
        # Read the text file[pattern] into a PCollection.
        lines = p | ReadFromText(known_args.input, coder=JsonCoder())

        load = (
                lines
                | beam.ParDo(Formatter())
                | beam.io.avroio.WriteToAvro(known_args.avro_output, schema=avro_schema, file_name_suffix='.avro', use_fastavro=True))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
