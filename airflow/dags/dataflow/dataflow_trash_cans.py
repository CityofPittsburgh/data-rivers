from __future__ import absolute_import

import argparse
import avro
import fastavro
import json
import logging
import os

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.avroio import WriteToAvro
from apache_beam.io.gcp.gcsio import GcsIO
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from fastavro import parse_schema, writer

from datetime import datetime
from google.cloud import storage

GOOGLE_APPLICATION_CREDENTIALS = os.environ['GOOGLE_APPLICATION_CREDENTIALS']

# TODO: resolve schema parsing error when fastavro=true is passed to WriteToAvro

class ConvertToDicts(beam.DoFn):
    def process(self, datum):
        container_id, receptacle_model_id, assignment_date, last_updated_date, group_name, address, city, state, zip, \
        neighborhood, dpw_division, council_district, ward, fire_zone = datum.split(',')

        return [{
            'container_id': int(container_id),
            'receptacle_model_id': int(receptacle_model_id),
            'assignment_date': assignment_date.strip('"'),
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



class LoadToBigQuery(beam.DoFn):
    def process(self, p):
        """Values in the global namespace are not available within DoFns on the Dataflow worker, so have to
        import bigquery inside the function"""
        from google.cloud import bigquery
        dt = datetime.now()
        bq_client = bigquery.Client()
        dataset_id = 'trash_cans'
        staging_table = 'trash_cans_staging_{}'.format(dt.strftime('%Y%m%d'))
        dataset_ref = bq_client.dataset(dataset_id)
        uri = 'gs://pghpa_trash_cans/avro_output/avro_output*',
        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.AVRO

        load_job = bq_client.load_table_from_uri(
            uri, dataset_ref.table(staging_table), job_config=job_config
        )

        load_job.result()


def run(argv=None):
    dt = datetime.now()
    parser = argparse.ArgumentParser()

    parser.add_argument('--input',
                        dest='input',
                        default='gs://pghpa_trash_cans/{}/{}/{}_smart_trash_containers.csv'.format(dt.strftime('%Y'),
                                                                                          dt.strftime('%m').lower(),
                                                                                          dt.strftime("%Y-%m-%d")),
                        help='Input file to process.')
    parser.add_argument('--avro_output',
                        dest='avro_output',
                        default='gs://pghpa_trash_cans/avro_output/avro_output',
                        help='Output directory to write avro files.')

    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_args.extend([
        # Use runner=DataflowRunner to run in GCP environment, runner=DirectRunner to run locally
        '--runner=DirectRunner',
        # '--runner=DataflowRunner',
        '--project=data-rivers',
        '--staging_location=gs://pghpa_trash_cans/staging',
        '--temp_location=gs://pghpa_trash_cans/temp',
        '--job_name=trash_cans_dataflow',
    ])

    def download_schema(bucket_name, source_blob_name, destination_file_name):
        """Downloads avro schema from Cloud Storage"""
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(source_blob_name)

        blob.download_to_filename(destination_file_name)

    download_schema('pghpa_avro_schemas', 'trash_can_test.avsc', 'trash_can_test.avsc')

    SCHEMA_PATH = 'trash_can_test.avsc'
    avro_schema = avro.schema.parse(open(SCHEMA_PATH, 'rb').read())

    pipeline_options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=pipeline_options) as p:
        # Read the text file[pattern] into a PCollection.
        lines = p | ReadFromText(known_args.input, skip_header_lines=1)

        load = (
                lines
                | beam.ParDo(ConvertToDicts())
                | beam.io.avroio.WriteToAvro(known_args.avro_output, schema=avro_schema, file_name_suffix='.avro')
                | beam.ParDo(LoadToBigQuery()))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
