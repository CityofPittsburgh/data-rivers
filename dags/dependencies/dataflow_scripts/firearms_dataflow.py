from __future__ import absolute_import

import logging
import os

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.avroio import WriteToAvro

from dataflow_utils import dataflow_utils
from dataflow_utils.dataflow_utils import clean_csv_int, clean_csv_string, clean_csv_float, generate_args


# todo: load ndjson at gcs load stage

class ConvertToDicts(beam.DoFn):
    def process(self, datum):
        year, month, day_of_week, address1, address2, total_count, pistol_count, revolver_count, rifle_count, \
        shotgun_count, other_count, police_zone, long, lat = datum.split(',')

        address_full = clean_csv_string(address1) + ' ' + clean_csv_string(address2)

        return [{
            'year': clean_csv_int(year),
            'month': clean_csv_int(month),
            'day_of_week': clean_csv_int(day_of_week),
            'address': dataflow_utils.normalize_address(address_full),
            'total_count': clean_csv_int(total_count),
            'pistol_count': clean_csv_int(pistol_count),
            'revolver_count': clean_csv_int(revolver_count),
            'rifle_count': clean_csv_int(rifle_count),
            'shotgun_count': clean_csv_int(shotgun_count),
            'other_count': clean_csv_int(other_count),
            'long': clean_csv_float(long),
            'lat': clean_csv_float(lat)
        }]


def run(argv=None):

    known_args, pipeline_options, avro_schema = generate_args(
        job_name='firearms-dataflow',
        bucket='{}_firearm_seizures'.format(os.environ['GCS_PREFIX']),
        argv=argv,
        schema_name='firearm_seizures',
        runner='DataflowRunner'
    )

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
