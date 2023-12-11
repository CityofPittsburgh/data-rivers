from __future__ import absolute_import

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.operators.bigquery_table_delete_operator import BigQueryTableDeleteOperator
from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_month, get_ds_year, get_ds_day, default_args, \
    build_geo_coords_from_parcel_query, build_revgeo_time_bound_query

# This DAG will perform an extract and transformation of Property Tax Delinquency data from the Real Estate Oracle
# database. Once the data is extracted, it will be uploaded to BigQuery and geocoded by matching on parcel number.
# The final output will be stored as a CSV file in GCS and made available to WPRDC for public display.

COLS = "parc_num, address, billing_city, current_delq, prior_years, state_description, neighborhood, " \
       "council_district, ward, CAST(dpw_streets AS STRING) AS public_works_division, " \
       "CAST(ward AS STRING) AS pli_division, police_zone, fire_zone, longitude, latitude"

dag = DAG(
    'finance_property_tax_delinquency',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2023, 5, 17),
    catchup=False,
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year,
                          'get_ds_day': get_ds_day},
    max_active_runs=1
)

# initialize gcs locations and store endpoint names in variables
source = "finance"
bucket = f"gs://{os.environ['GCS_PREFIX']}_{source}"
dataset = "tax_delinquency"
exec_date = "{{ ds }}"
path = "{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ run_id }}"
json_loc = f"{dataset}/{path}_records.json"
table = f"property_{dataset}"
export_file = "{{ ds|get_ds_month }}-{{ ds|get_ds_year }}"

tax_delinquency_gcs = BashOperator(
    task_id='tax_delinquency_gcs',
    bash_command=f"python {os.environ['GCS_LOADER_PATH']}/{source}_{dataset}_gis_wprdc_extract.py --output_arg {json_loc}",
    dag=dag
)

# the primary key of tax delinquency data is parcel ID; parcel data is also stored in the timebound_geography dataset
# with corresponding geographical boundaries. this query uses the ST_CENTROID geographic function to obtain lat/longs
# for each parcel
sub_query = build_geo_coords_from_parcel_query(f"{os.environ['GCLOUD_PROJECT']}.{source}.incoming_{table}", "parc_num")
coord_query = f"CREATE OR REPLACE TABLE  `{os.environ['GCLOUD_PROJECT']}.{source}.incoming_{table}` AS {sub_query}"
get_coords = BigQueryOperator(
    task_id='get_coords',
    sql=coord_query,
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

# the following task joins the parcel records to the geographic zones displayed in the published WPRDC dataset
geojoin = BigQueryOperator(
    task_id='geojoin',
    sql=build_revgeo_time_bound_query(source, f"incoming_{table}", f"geo_enriched_{table}", 'modify_date',
                                      'latitude', 'longitude'),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

query_create_partition = F"""
CREATE OR REPLACE TABLE `{os.environ['GCLOUD_PROJECT']}.{source}.{dataset}_partitioned` 
partition by DATE_TRUNC(modify_date, MONTH) AS
SELECT 
* EXCEPT (neighborhood, neighborhood_name, modify_date),
    COALESCE(neighborhood_name, neighborhood) AS neighborhood,
    PARSE_DATETIME('%Y-%m-%d %H:%M:%S', modify_date) as modify_date
FROM 
  `{os.environ['GCLOUD_PROJECT']}.{source}.geo_enriched_{table}`;"""
create_partition = BigQueryOperator(
    task_id='create_partition',
    sql=query_create_partition,
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

# export the final dataset to a new table that will be converted to a CSV and provided to WPRDC
# modify_date is excluded as that field is not present in the published dataset
query_export_cols = f"""
CREATE OR REPLACE TABLE `{os.environ['GCLOUD_PROJECT']}.{source}.delinquency_export_wprdc` AS
SELECT
    {COLS}
FROM
    `{os.environ['GCLOUD_PROJECT']}.{source}.{dataset}_partitioned`
"""
create_export_table = BigQueryOperator(
    task_id='create_export_table',
    sql=query_export_cols,
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

# export table as CSV to WPRDC bucket
wprdc_export = BigQueryToCloudStorageOperator(
    task_id='wprdc_export',
    source_project_dataset_table=f"{os.environ['GCLOUD_PROJECT']}.{source}.delinquency_export_wprdc",
    destination_cloud_storage_uris=[f"gs://{os.environ['GCS_PREFIX']}_wprdc/{source}/{dataset}/{export_file}.csv"],
    bigquery_conn_id='google_cloud_default',
    dag=dag
)

# clean up BQ by removing temporary WPRDC table after it has been converted to a CSV
delete_export = BigQueryTableDeleteOperator(
    task_id="delete_export",
    deletion_dataset_table=f"{os.environ['GCLOUD_PROJECT']}.{source}.delinquency_export_wprdc",
    bigquery_conn_id='google_cloud_default',
    dag=dag
)

# push table to data-bridGIS BQ
query_push_gis = F"""
CREATE OR REPLACE TABLE `data-bridgis.{source}.{dataset}_partitioned` AS
SELECT 
* 
FROM 
  `{os.environ['GCLOUD_PROJECT']}.{source}.delinquency_export_wprdc`;
"""
push_gis = BigQueryOperator(
    task_id='push_gis',
    sql=query_push_gis,
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

tax_delinquency_gcs >> get_coords >> geojoin >> create_partition >> create_export_table
create_export_table >> wprdc_export >> delete_export
create_export_table >> push_gis >> delete_export
