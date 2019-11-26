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
from . import dataflow_utils.dataflow_utils
from .dataflow_utils.dataflow_utils import hash_func, download_schema, clean_csv_int, clean_csv_string


class ConvertToDicts(beam.DoFn):
    def process(self, datum):

        container_id, receptacle_model_id, assignment_date, last_updated_date, group_name, address, city, state, zip, \
        neighborhood, dpw_division, council_district, ward, fire_zone = datum.split(',')

        return [{
            'container_id': clean_csv_int(container_id),
            'receptacle_model_id': clean_csv_int(receptacle_model_id),
            'assignment_date': clean_csv_string(assignment_date),
            'last_updated_date': last_updated_date.strip('"'),
            'group_name': group_name.strip('"'),
            'address': address.strip('"'),
            'city': city.strip('"'),
            'state': state.strip('"'),
            'zip': int(zip),
            'neighborhood': neighborhood.strip('"'),
            'dpw_division': int(dpw_division),
            'council_district': int(council_district),
            'ward': int(ward),
            'fire_zone': fire_zone.strip('"')
        }]


def run(argv=None):
    dt = datetime.now()
    parser = argparse.ArgumentParser()

    parser.add_argument('--input',
                        dest='input',
                        default='gs://pghpa_finance/{}/{}/{}_smart_trash_containers.csv'.format(dt.strftime('%Y'),
                                                                                          dt.strftime('%m').lower(),
                                                                                          dt.strftime("%Y-%m-%d")),
                        help='Input file to process.')
    parser.add_argument('--avro_output',
                        dest='avro_output',
                        default='gs://pghpa_finance/avro_output/avro_output',
                        help='Output directory to write avro files.')

    known_args, pipeline_args = parser.parse_known_args(argv)

    #TODO: run on on-prem network when route is opened

    pipeline_args.extend([
        # Use runner=DataflowRunner to run in GCP environment, runner=DirectRunner to run locally
        # '--runner=DirectRunner',
        '--runner=DataflowRunner',
        '--project=data-rivers',
        '--staging_location=gs://pghpa_finance/staging',
        '--temp_location=gs://pghpa_finance/temp',
        '--job_name=registered-businesses-dataflow',
        '--subnetwork=https://www.googleapis.com/compute/v1/projects/data-rivers/regions/us-east1/subnetworks/default',
        '--region=us-east1',
        '--service_account_email=data-rivers@data-rivers.iam.gserviceaccount.com'
        # '--subnetwork=https://www.googleapis.com/compute/v1/projects/data-rivers/regions/us-east1/subnetworks/pgh-on-prem-net-142'
    ])

    download_schema('pghpa_avro_schemas', 'registered_businesses.avsc', 'registered_businesses.avsc')

    SCHEMA_PATH = 'registered_businesses.avsc'
    # fastavro does the work of avro.schema.parse(), just need to pass dict
    avro_schema = json.loads(open(SCHEMA_PATH).read())

    pipeline_options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=pipeline_options) as p:
        # Read the text file[pattern] into a PCollection.
        lines = p | ReadFromText(known_args.input, skip_header_lines=1)

        load = (
                lines
                | beam.ParDo(ConvertToDicts())
                | beam.io.avroio.WriteToAvro(known_args.avro_output, schema=avro_schema, file_name_suffix='.avro', use_fastavro=True))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
