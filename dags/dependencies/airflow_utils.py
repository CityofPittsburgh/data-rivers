from __future__ import absolute_import

import logging
import os

from datetime import datetime, timedelta
from google.cloud import bigquery, storage

dt = datetime.now()
yesterday = datetime.combine(dt - timedelta(1), datetime.min.time())

bq_client = bigquery.Client()
storage_client = storage.Client()

default_args = {
    'depends_on_past': False,
    'start_date': yesterday,
    'email': os.environ['EMAIL'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'project_id': os.environ['GCLOUD_PROJECT'],
    'dataflow_default_options': {
        'project': os.environ['GCLOUD_PROJECT']
    }
}

def get_ds_year(ds):
    return ds.split('-')[0]


def get_ds_month(ds):
    return ds.split('-')[1]


def build_revgeo_query(dataset, temp_table, id_field):
    """
    Take a table with lat/long values and reverse-geocode it into a new a final table. Use UNION to include rows that
    can't be reverse-geocoded in the final table. SELECT DISTINCT in both cases to remove duplicates.

    :param dataset: BigQuery dataset (string)
    :param temp_table: non-reverse geocoded table (string)
    :param id_field: field in table to use for deduplication
    :return: string to be passed through as arg to BigQueryOperator
    """
    return f"""
    WITH {temp_table}_geo AS 
    (
       SELECT DISTINCT
          {temp_table}.*,
          neighborhoods.hood AS neighborhood,
          council_districts.council AS council_district,
          wards.ward,
          fire_zones.firezones AS fire_zone,
          police_zones.zone AS police_zone,
          dpw_divisions.objectid AS dpw_division 
       FROM
          `{os.environ['GCLOUD_PROJECT']}.{dataset}.{temp_table}` AS {temp_table} 
          JOIN
             `data-rivers.geography.neighborhoods` AS neighborhoods 
             ON ST_CONTAINS(neighborhoods.geometry, ST_GEOGPOINT({temp_table}.long, {temp_table}.lat)) 
          JOIN
             `data-rivers.geography.council_districts` AS council_districts 
             ON ST_CONTAINS(council_districts.geometry, ST_GEOGPOINT({temp_table}.long, {temp_table}.lat)) 
          JOIN
             `data-rivers.geography.wards` AS wards 
             ON ST_CONTAINS(wards.geometry, ST_GEOGPOINT({temp_table}.long, {temp_table}.lat)) 
          JOIN
             `data-rivers.geography.fire_zones` AS fire_zones 
             ON ST_CONTAINS(fire_zones.geometry, ST_GEOGPOINT({temp_table}.long, {temp_table}.lat)) 
          JOIN
             `data-rivers.geography.police_zones` AS police_zones 
             ON ST_CONTAINS(police_zones.geometry, ST_GEOGPOINT({temp_table}.long, {temp_table}.lat)) 
          JOIN
             `data-rivers.geography.dpw_divisions` AS dpw_divisions 
             ON ST_CONTAINS(dpw_divisions.geometry, ST_GEOGPOINT({temp_table}.long, {temp_table}.lat))
    )
    SELECT
       * 
    FROM
       {temp_table}_geo 
    UNION ALL
    SELECT DISTINCT
       {temp_table}.*,
       CAST(NULL AS string) AS neighborhood,
       NULL AS council_district,
       NULL AS ward,
       CAST(NULL AS string) fire_zone,
       NULL AS police_zone,
       NULL AS dpw_division 
    FROM
       `{os.environ['GCLOUD_PROJECT']}.{dataset}.{temp_table}` AS {temp_table} 
    WHERE
       {temp_table}.{id_field} NOT IN 
       (
          SELECT
             {id_field} 
          FROM
             {temp_table}_geo
       )
    """


def dedup_table(dataset, table):
    return f"""
    SELECT DISTINCT * FROM `{os.environ['GCLOUD_PROJECT']}.{dataset}.{table}`
    """


def filter_old_values(dataset, temp_table, final_table, join_field):
    return f"""
    DELETE FROM `{os.environ['GCLOUD_PROJECT']}.{dataset}.{final_table}` final
    WHERE final.{join_field} IN (SELECT {join_field} FROM `{os.environ['GCLOUD_PROJECT']}.{dataset}.{temp_table}`)
    """


def beam_cleanup_statement(bucket):
    return "if gsutil -q stat gs://{}/beam_output/*; then gsutil rm gs://{}/beam_output/**; else echo " \
           "no beam output; fi".format(bucket, bucket)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
