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

GOOGLE_APPLICATION_CREDENTIALS = "/Users/jamesotoole/google-cloud-sdk/service_account_keys/data_rivers_key.json"
# can use bq client library method from_service_account_json(json_credentials_path, *args, **kwargs) to pull in prod

# TODO: run command to download creds, plug them into cloud composer, or access them from cloud storage bucket
# TODO: use resolving schema parsing error when fastavro=true is passed to WriteToAvro

class LoadToBigQuery(beam.DoFn):
    def process(self, p):
        """Values in the global namespace are not available within DoFns on the Dataflow worker, so have to
        import bigquery inside the function"""
        from google.cloud import bigquery
        dt = datetime.now()
        bq_client = bigquery.Client()
        dataset_id = 'trash_cans'
        staging_table = 'trash_cans_staging_{}'.format(dt.strftime('%m-%d-%y'))
        dataset_ref = bq_client.dataset(dataset_id)
        uri = 'gs://pghpa-trash-cans/avro_output/avro_output*',
        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.AVRO

        load_job = bq_client.load_table_from_uri(
            uri, dataset_ref.table(staging_table), job_config=job_config
        )

        load_job.result()


class EnrichQueryResults(beam.DoFn):
    def process(self):
        query = """
            SELECT
              trash_cans.*,
              neighborhoods.hood AS neighborhood,
              council_districts.council_district,
              wards.ward,
              fire_zones.firezones AS fire_zone,
              dpw_divisions.objectid AS dpw_division
            FROM
              trash_cans.trash_cans_staging AS trash_cans
            JOIN
              geography.neighborhoods
            ON
              ST_CONTAINS(neighborhoods.geometry,
                ST_GeogPoint(trash_cans.Longitude,
                  trash_cans.Latitude))
            JOIN
              geography.council_districts
            ON
              ST_CONTAINS(council_districts.geometry,
                ST_GeogPoint(trash_cans.Longitude,
                  trash_cans.Latitude))
            JOIN
              geography.wards
            ON
              ST_CONTAINS(wards.geometry,
                ST_GeogPoint(trash_cans.Longitude,
                  trash_cans.Latitude))
            JOIN
              geography.fire_zones
            ON
              ST_CONTAINS(fire_zones.geometry,
                ST_GeogPoint(trash_cans.Longitude,
                  trash_cans.Latitude))
            JOIN
              geography.dpw_divisions
            ON
              ST_CONTAINS(dpw_divisions.geometry,
                ST_GeogPoint(trash_cans.Longitude,
                  trash_cans.Latitude))
        """

        enriched_table_ref = dataset_ref.table('trash_cans_enriched')
        enriched_table = bigquery.Table(enriched_table_ref)
        query_job_config = bigquery.QueryJobConfig()
        query_job_config.destination = enriched_table
        query_job_config.time_partitioning = bigquery.TimePartitioning()
        query_job_config.allow_large_results = True

        query_job = bq_client.query(query=query, job_config=query_job_config)

        query_job.result()



        # DROP TABLE trash_cans_staging


class FinalResultsToGCS(beam.DoFn):
    def process(self, p):
        from google.cloud import bigquery
        bq_client = bigquery.Client()
        dataset_id = 'trash_cans'
        table = 'trash_cans_enriched'
        dataset_ref = bq_client.dataset(dataset_id)
        dt = datetime.now()
        uri = 'gs://pghpa-trash-cans/{}/{}/{}_containers_enriched*'.format(dt.strftime('%Y'), dt.strftime('%B').lower(),
                                                                           dt.strftime("%m-%d-%y"))

        job_config = bigquery.ExtractJobConfig()
        job_config.destination_format = 'NEWLINE_DELIMITED_JSON'
        load_job = bq_client.extract_table(dataset_ref.table(table, uri, job_config=job_config))
        load_job.result()
        return


def run(argv=None):
    parser = argparse.ArgumentParser()

    parser.add_argument('--input',
                        dest='input',
                        # default = 'gs://pghpa-trash-cans/{}/{}/{}_containers.json'.format(dt.strftime('%Y'),
                        #                                                                   dt.strftime('%B').lower(),
                        #                                                                   dt.strftime("%m-%d-%y"))',
                        default='gs://pghpa-trash-cans/2019/september/09-04-19_containers.json',
                        help='Input file to process.')
    parser.add_argument('--avro_output',
                        dest='avro_output',
                        default='gs://pghpa-trash-cans/avro_output/avro_output',
                        help='Output directory to write avro files.')

    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_args.extend([
        # Use runner=DataflowRunner to run in GCP environment, runner=DirectRunner to run locally
        # '--runner=DataflowRunner',
        '--runner=DirectRunner',
        '--project=data-rivers',
        '--staging_location=gs://pghpa-trash-cans/staging',
        '--temp_location=gs://pghpa-trash-cans/temp',
        '--job_name=trash-cans-test',
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
        lines = p | ReadFromText(known_args.input)

        load = (
                lines
                | 'Convert to dicts' >> beam.Map(lambda text: json.loads(text))
                | beam.io.avroio.WriteToAvro(known_args.avro_output, schema=avro_schema, file_name_suffix='.avro')
                | 'Write Avro to BigQuery and join with location data' >> beam.ParDo(LoadToBigQuery()))

        # join = p | 'Join with location table' >> beam.io.Read(beam.io.BigQuerySource(query=query))
        #
        # export = (
        #         join
        #         | 'Load to enriched table' >> beam.

        #         | 'Write enriched data to GCS' >> beam.DoFn(FinalResultsToGCS()))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
