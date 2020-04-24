from __future__ import absolute_import

import argparse
import logging
import os

import apache_beam as beam
import dataflow_utils

from apache_beam.io import ReadFromText
from apache_beam.io.avroio import WriteToAvro
from apache_beam.options.pipeline_options import PipelineOptions

from dataflow_utils import clean_csv_int, clean_csv_string, generate_args, get_schema, dt


class ConvertToDicts(beam.DoFn):
    def process(self, datum):
        date, center_name, attendance_count = datum.split(',')

        return [{
            'date': clean_csv_string(date),
            'center_name': clean_csv_string(center_name),
            'attendance_count': clean_csv_int(attendance_count)
        }]


def run(argv=None):
    parser = argparse.ArgumentParser()

    parser.add_argument('--input',
                        dest='input',
                        default='gs://{}_community_centers/attendance/{}/{}/{}_attendance.csv'.format(
                            os.environ['GCS_PREFIX'],
                            dt.strftime('%Y'),
                            dt.strftime('%m').lower(),
                            dt.strftime("%Y-%m-%d")),
                        help='Input file to process.')
    parser.add_argument('--avro_output',
                        dest='avro_output',
                        default='gs://{}_community_centers/attendance/avro_output/{}/{}/{}/avro_output'.format(
                            os.environ['GCS_PREFIX'],
                            dt.strftime('%Y'),
                            dt.strftime('%m').lower(),
                            dt.strftime("%Y-%m-%d")),
                        help='Output directory to write avro files.')

    known_args, pipeline_args = parser.parse_known_args(argv)

    # TODO: run on on-prem network when route is opened
    # Use runner=DataflowRunner to run in GCP environment, DirectRunner to run locally
    pipeline_args.extend(generate_args('comm-ctr-attendance-dataflow',
                                       '{}_community_centers'.format(os.environ['GCS_PREFIX']),
                                       'DirectRunner'))

    avro_schema = get_schema('community_center_attendance')

    pipeline_options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=pipeline_options) as p:
        # Read the text file[pattern] into a PCollection.
        lines = p | ReadFromText(known_args.input, skip_header_lines=1)

        load = (
                lines
                | beam.ParDo(ConvertToDicts())
                | WriteToAvro(known_args.avro_output, schema=avro_schema, file_name_suffix='.avro', use_fastavro=True))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
