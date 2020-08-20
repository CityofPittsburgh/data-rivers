from __future__ import absolute_import

import logging
import os

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.avroio import WriteToAvro

from dataflow_utils import dataflow_utils
from dataflow_utils.dataflow_utils import generate_args, JsonCoder


# TODO: pass input/output buckets as params from DataflowPythonOperator in DAG

def run(argv=None):

    known_args, pipeline_options, avro_schema = generate_args(
        job_name='twilio-311-dataflow',
        bucket='{}_twilio'.format(os.environ['GCS_PREFIX']),
        argv=argv,
        schema_name='twilio_reports'
    )

    with beam.Pipeline(options=pipeline_options) as p:
        # Read the text file[pattern] into a PCollection.
        lines = p | ReadFromText(known_args.input, coder=JsonCoder())

        load = (
                lines
                | WriteToAvro(known_args.avro_output, schema=avro_schema, file_name_suffix='.avro', use_fastavro=True))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
