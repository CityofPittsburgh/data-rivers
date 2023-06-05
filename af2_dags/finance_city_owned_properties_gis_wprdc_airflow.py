from __future__ import absolute_import

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator

from dependencies.airflow_utils import get_ds_month, get_ds_year, default_args, \
    build_revgeo_time_bound_query, build_geo_coords_from_parcel_query

# This DAG will perform an extract and transformation of City Owned Properties from the Real Estate Oracle
# database. Once the data is extracted, it will be uploaded to BigQuery and geocoded by matching on parcel number.
# The final output will be stored as a JSON file in GCS and made available to WPRDC for public display.

COLS = "pin, modify_date, address, billing_city, current_delq, prior_years, state_description, neighborhood"


dag = DAG(
    'finance_city_owned_properties ',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2023, 6, 4),
    catchup=False,
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year},
    max_active_runs=1
)


data_name = "city_owned_properties"
path = "{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ run_id }}"
json_loc = f"{data_name}/{path}_city_owned_properties.json"


extract = BashOperator(
    task_id='extract',
    bash_command=f"python {os.environ['GCS_LOADER_PATH']}/finance_city_owned_properties_gis_wprdc_extract.py "
                 f"--output_arg {json_loc}",
    dag=dag
)


# the primary key of city owned properties is parcel ID; parcel data is also stored in the timebound_geography dataset
# with corresponding geographical boundaries. this query uses the ST_CENTROID geographic function to obtain lat/longs
# for each parcel
query_coords = build_geo_coords_from_parcel_query(raw_table = F"{os.environ['GCLOUD_PROJECT']}.finance.incoming_city_owned_properties",
                                                  parc_field = "pin")
query_coords = F""" CREATE OR REPLACE TABLE {os.environ['GCLOUD_PROJECT']}.finance.incoming_city_owned_properties AS
{query_coords}"""
get_coords = BigQueryOperator(
    task_id='get_coords',
    sql=query_coords,
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

























# ToDo: get a date field
query_geo_join = build_revgeo_time_bound_query('finance', 'incoming_city_owned_properties',
                                               'geo_enriched_city_owned_properties',
                                               'approved_date_UTC', 'pin', 'latitude', 'longitude',
                                               geo_fields_in_raw = False)
geojoin = BigQueryOperator(
        task_id = 'geojoin',
        sql = query_geo_join,
        bigquery_conn_id='google_cloud_default',
        use_legacy_sql = False,
        dag = dag
)


query_create_partition = F"""CREATE OR REPLACE TABLE `{os.environ['GCLOUD_PROJECT']}.finance.tax_abatement_partitioned` 
partition by DATE_TRUNC(partition_approved_date_UTC, MONTH) AS
SELECT 
* EXCEPT(approved_date_UTC, start_year),
CAST (start_year AS string) as start_year,
PARSE_DATE ("%Y-%m-%d", approved_date_UTC) as partition_approved_date_UTC
FROM 
  `{os.environ['GCLOUD_PROJECT']}.finance.geo_enriched_tax_abatement`;"""
create_partition = BigQueryOperator(
        task_id = 'create_partition',
        sql = query_create_partition,
        bigquery_conn_id = 'google_cloud_default',
        use_legacy_sql = False,
        dag = dag
)


# Export table as CSV to WPRDC bucket
# file name is the date. path contains the date info
csv_file_name = f"{path}"
dest_bucket = f"gs://{os.environ['GCS_PREFIX']}_wprdc/finance/tax_abatement/"
wprdc_export = BigQueryToCloudStorageOperator(
        task_id = 'wprdc_export',
        source_project_dataset_table = f"{os.environ['GCLOUD_PROJECT']}.finance.tax_abatement_partitioned",
        destination_cloud_storage_uris = [f"{dest_bucket}{csv_file_name}.csv"],
        dag = dag
)


# push table to data-bridGIS BQ
query_push_gis = F"""
CREATE OR REPLACE TABLE `data-bridgis.finance.tax_abatement_partitioned` AS
SELECT 
* 
FROM 
  `{os.environ['GCLOUD_PROJECT']}.finance.tax_abatement_partitioned`;
"""
push_gis = BigQueryOperator(
        task_id = 'push_gis',
        sql = query_push_gis,
        bigquery_conn_id = 'google_cloud_default',
        use_legacy_sql = False,
        dag = dag
)


extract >> get_coords >> geojoin >> create_partition
create_partition >> wprdc_export
create_partition >> push_gis