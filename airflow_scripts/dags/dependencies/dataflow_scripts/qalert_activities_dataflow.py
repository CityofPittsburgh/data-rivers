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
    """
    If you want to run just this file for rapid development, change runner to 'DirectRunner' and add
    GCS paths for --input and --avro_output, e.g.
    python qalert_requests_dataflow.py --input gs://pghpa_test_qalert/requests/2020/06/2020-06-17_requests.json
    --avro_output gs://pghpa_test_qalert/requests/avro_output/2020/06/2020-06-17/
    """

    known_args, pipeline_options, avro_schema = generate_args(
        job_name='qalert-activities-dataflow',
        bucket='{}_qalert'.format(os.environ['GCS_PREFIX']),
        argv=argv,
        schema_name='City_of_Pittsburgh_QAlert_Activities',
        runner='DataflowRunner'
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
