from __future__ import absolute_import

import argparse
import logging
import os

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.avroio import WriteToAvro
from apache_beam.options.pipeline_options import PipelineOptions

from dataflow_utils import dataflow_utils
from dataflow_utils.dataflow_utils import generate_args, get_schema, JsonCoder


## TODO: pass input/output buckets as params from DataflowPythonOperator in DAG

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', dest='input', required=True)
    parser.add_argument('--avro_output', dest='avro_output', required=True)
    # parser.add_argument('--input',
    #                     dest='input',
    #                     default='gs://{}_qalert/activities/{}/{}/{}_activities.json'.format(os.environ['GCS_PREFIX'],
    #                                                                                         dt.strftime('%Y'),
    #                                                                                         dt.strftime('%m').lower(),
    #                                                                                         dt.strftime("%Y-%m-%d")),
    #                     help='Input file to process.')
    # parser.add_argument('--avro_output',
    #                     dest='avro_output',
    #                     default='gs://{}_qalert/activities/avro_output/{}/{}/{}/avro_output'.format(
    #                         os.environ['GCS_PREFIX'],
    #                         dt.strftime('%Y'),
    #                         dt.strftime('%m').lower(),
    #                         dt.strftime("%Y-%m-%d")),
    #                     help='Output directory to write avro files.')

    known_args, pipeline_args = parser.parse_known_args(argv)

    # Use runner=DirectRunner for rapid development locally if needed
    pipeline_args.extend(generate_args('qalert-activity-dataflow',
                                       '{}_qalert'.format(os.environ['GCS_PREFIX']),
                                       'DataflowRunner'))

    pipeline_args.append('--setup_file={}'.format(os.environ['SETUP_PY_DATAFLOW']))

    avro_schema = get_schema('City_of_Pittsburgh_QAlert_Activities')

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
