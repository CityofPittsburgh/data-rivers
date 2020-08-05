from __future__ import absolute_import

import logging
import os

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.avroio import WriteToAvro

from dataflow_utils import dataflow_utils
from dataflow_utils.dataflow_utils import clean_csv_int, clean_csv_string, generate_args, get_schema


class ConvertToDicts(beam.DoFn):
    def process(self, datum):
        container_id, receptacle_model_id, assignment_date, last_updated_date, group_name, address, city, state, \
        zip, neighborhood, dpw_division, council_district, ward, fire_zone = datum.split(',')

        return [{
            'container_id': clean_csv_int(container_id),
            'receptacle_model_id': clean_csv_int(receptacle_model_id),
            'assignment_date': clean_csv_string(assignment_date),
            'last_updated_date': clean_csv_string(last_updated_date),
            'group_name': clean_csv_string(group_name),
            'address': clean_csv_string(address),
            'city': clean_csv_string(city),
            'state': clean_csv_string(state),
            'zip': clean_csv_int(zip),
            'neighborhood': clean_csv_string(neighborhood),
            'dpw_division': clean_csv_int(dpw_division),
            'council_district': clean_csv_int(council_district),
            'ward': clean_csv_int(ward),
            'fire_zone': clean_csv_string(fire_zone)
        }]


def run(argv=None):

    known_args, pipeline_options, avro_schema = generate_args(
        job_name='trash-cans-dataflow',
        bucket='{}_trash_cans'.format(os.environ['GCS_PREFIX']),
        argv=argv,
        schema_name='smart_trash_cans',
        runner='DataflowRunner'
    )

    with beam.Pipeline(options=pipeline_options) as p:
        lines = p | ReadFromText(known_args.input, skip_header_lines=1)

        load = (
                lines
                | beam.ParDo(ConvertToDicts())
                | WriteToAvro(known_args.avro_output, schema=avro_schema, file_name_suffix='.avro', use_fastavro=True))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
