from __future__ import absolute_import

import logging
import os

from datetime import datetime, timedelta
from google.cloud import bigquery, storage


dt = datetime.now()
yesterday = datetime.combine(datetime.today() - timedelta(1), datetime.min.time())
week_ago = datetime.combine(datetime.today() - timedelta(7), datetime.min.time())
last_day_prev_month = dt.replace(day=1) - timedelta(days=1)
first_day_prev_month = dt.replace(day=1) - timedelta(days=last_day_prev_month.day)

GOOGLE_APPLICATION_CREDENTIALS = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
bq_client = bigquery.Client()
storage_client = storage.Client()

#TODO: When Airflow 2.0 is released, upgrade the package, upgrade the virtualenv to Python3,
# and add the arg py_interpreter='python3' to DataFlowPythonOperator


def load_avro_to_bq(dataset, table, gcs_bucket, date_partition=False, partition_by=None):
    dataset_id = dataset
    table = table
    dataset_ref = bq_client.dataset(dataset_id)
    uri = 'gs://{}}/avro_output/avro_output*'.format(gcs_bucket)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.AVRO
    if date_partition == True:
        job_config.time_partitioning = bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.DAY,
                                                                 field=partition_by)

    load_job = bq_client.load_table_from_uri(
        uri, dataset_ref.table(table), job_config=job_config
    )

    load_job.result()
    print('Data loaded to {} table, {} dataset'.format(table, dataset))


def cleanup_beam_avro(bucket_name):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)

    avro = bucket.blob('avro_output/*')
    avro.delete()
    print('Avro files deleted')

    beam_temp = bucket.blob('temp/*')
    beam_temp.delete()
    print('Beam temp files deleted')

    beam_staging = bucket.blob('staging/*')
    beam_staging.delete()
    print('Beam staging files deleted')


def geocode_address(address):
    query = """
    SELECT
      geometry
    FROM
      geography.pittsburgh_addresses AS addresses
    WHERE
      normalized_address = {}
    LIMIT
        1 
    """.format(address)
    bq_client = bigquery.Client()
    job_config = bigquery.QueryJobConfig()
    query_job = bq_client.query(query)

    result = query_job.result()
    point = result[0]
    return point


def build_revgeo_query(dataset, temp_table):
    return f"""
    SELECT
        {temp_table}.*,
        neighborhoods.hood AS neighborhood,
        council_districts.council_district,
        wards.ward,
        fire_zones.firezones AS fire_zone,
        police_zones.zone AS police_zone,
        dpw_divisions.objectid AS dpw_division
    FROM
      `{os.environ['GCP_PROJECT']}.{dataset}.{temp_table}` AS {temp_table}
    JOIN
      `data-rivers.geography.neighborhoods` AS neighborhoods
    ON
      ST_CONTAINS(neighborhoods.geometry,
        ST_GEOGPOINT({temp_table}.long,
          {temp_table}.lat))
    JOIN
      `data-rivers.geography.council_districts` AS council_districts
    ON
      ST_CONTAINS(council_districts.geometry,
        ST_GEOGPOINT({temp_table}.long,
          {temp_table}.lat))
    JOIN
      `data-rivers.geography.wards` AS wards
    ON
      ST_CONTAINS(wards.geometry,
        ST_GEOGPOINT({temp_table}.long,
          {temp_table}.lat))
    JOIN
      `data-rivers.geography.fire_zones` AS fire_zones
    ON
      ST_CONTAINS(fire_zones.geometry,
        ST_GEOGPOINT({temp_table}.long,
          {temp_table}.lat))
    JOIN
      `data-rivers.geography.police_zones` AS police_zones
    ON
      ST_CONTAINS(police_zones.geometry,
        ST_GEOGPOINT({temp_table}.long,
          {temp_table}.lat))
    JOIN
      `data-rivers.geography.dpw_divisions` AS dpw_divisions
    ON
      ST_CONTAINS(dpw_divisions.geometry,
        ST_GEOGPOINT({temp_table}.long,
          {temp_table}.lat))
    """


def beam_cleanup_statement(bucket):
    return "if gsutil -q stat gs://{}/beam_output/*; then gsutil rm gs://{}/beam_output/**; else echo " \
           "no beam output; fi".format(bucket, bucket)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
