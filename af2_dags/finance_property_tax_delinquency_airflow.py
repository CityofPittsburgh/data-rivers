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
    build_insert_new_records_query, build_format_dedup_query, build_revgeo_time_bound_query, build_sync_update_query, \
    build_piecemeal_revgeo_query

# This DAG will perform an extract and transformation of Property Tax Delinquency data from the Real Estate Oracle
# database. Once the data is extracted, it will be uploaded to BigQuery and geocoded by matching on parcel number.
# The final output will be stored as a CSV file in GCS and made available to WPRDC for public display.

COLS = "pin, modify_date, address, billing_city, current_delq, prior_years, state_description, neighborhood"
FINAL_COLS = COLS + ", council_district, ward, public_works_division, police_zone, fire_zone, longitude, latitude"
UPD_COLS = ["modify_date", "current_delq", "prior_years", "state_description"]

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
geo_config = [{'geo_table': 'council_districts', 'geo_field': 'council_district'},
              {'geo_table': 'wards', 'geo_field': 'ward'},
              {'geo_table': 'dpw_streets_divisions', 'geo_field': 'public_works_division'},
              {'geo_table': 'police_zones', 'geo_field': 'police_zone'},
              {'geo_table': 'fire_zones', 'geo_field': 'fire_zone'}]

tax_delinquency_gcs = BashOperator(
    task_id='tax_delinquency_gcs',
    bash_command=f"python {os.environ['GCS_LOADER_PATH']}/finance_property_tax_delinquency_gcs.py --output_arg {json_loc}",
    dag=dag
)

# the primary key of tax delinquency data is parcel ID; parcel data is also stored in the timebound_geography dataset
# with corresponding geographical boundaries. this query uses the ST_CENTROID geographic function to obtain lat/longs
# for each parcel
coord_query = F"""
CREATE OR REPLACE TABLE `{os.environ['GCLOUD_PROJECT']}.{source}.incoming_{table}` AS
    SELECT {COLS}, NULL AS neighborhood_name, NULL AS council_district, NULL AS ward, 
           NULL AS public_works_division, NULL AS police_zone, NULL AS fire_zone, 
           ST_Y(ST_CENTROID(p.geometry)) AS latitude, ST_X(ST_CENTROID(p.geometry)) AS longitude
      FROM `{os.environ['GCLOUD_PROJECT']}.{source}.incoming_{table}`
      LEFT OUTER JOIN `{os.environ['GCLOUD_PROJECT']}.timebound_geography.parcels` p
        ON pin = p.zone"""
get_coords = BigQueryOperator(
    task_id='get_coords',
    sql=coord_query,
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

# the following 5 tasks join the parcel records to the geographic zones displayed in the published WPRDC dataset
geojoin_council = BigQueryOperator(
    task_id='geojoin_council',
    sql=build_piecemeal_revgeo_query(source, f'incoming_{table}', f'incoming_{table}', 'modify_date', 'pin',
                                     'latitude', 'longitude', geo_config[0]['geo_table'], geo_config[0]['geo_field']),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

geojoin_ward = BigQueryOperator(
    task_id='geojoin_ward',
    sql=build_piecemeal_revgeo_query(source, f'incoming_{table}', f'incoming_{table}', 'modify_date', 'pin',
                                     'latitude', 'longitude', geo_config[1]['geo_table'], geo_config[1]['geo_field']),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

geojoin_dpw = BigQueryOperator(
    task_id='geojoin_dpw',
    sql=build_piecemeal_revgeo_query(source, f'incoming_{table}', f'incoming_{table}', 'modify_date', 'pin',
                                     'latitude', 'longitude', geo_config[2]['geo_table'], geo_config[2]['geo_field']),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

geojoin_police = BigQueryOperator(
    task_id='geojoin_police',
    sql=build_piecemeal_revgeo_query(source, f'incoming_{table}', f'incoming_{table}', 'modify_date', 'pin',
                                     'latitude', 'longitude', geo_config[3]['geo_table'], geo_config[3]['geo_field']),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

geojoin_fire = BigQueryOperator(
    task_id='geojoin_fire',
    sql=build_piecemeal_revgeo_query(source, f'incoming_{table}', f'incoming_{table}', 'modify_date', 'pin',
                                     'latitude', 'longitude', geo_config[4]['geo_table'], geo_config[4]['geo_field']),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

# insert records for delinquent properties that don't already exist in the final dataset
insert_new = BigQueryOperator(
    task_id='insert_new',
    sql=build_insert_new_records_query(source, f"incoming_{table}", table, "pin", FINAL_COLS),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

# update details for delinquent properties if they are already present in the final dataset
update_changed = BigQueryOperator(
    task_id='update_changed',
    sql=build_sync_update_query(source, table, f"incoming_{table}", "pin", UPD_COLS),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

# export the final dataset to a new table that will be converted to a CSV and provided to WPRDC
# modify_date is excluded as that field is not present in the published dataset
query_export_cols = f"""
CREATE OR REPLACE TABLE `{os.environ['GCLOUD_PROJECT']}.{source}.data_export_wprdc` AS
SELECT
    {FINAL_COLS.replace('modify_date, ', '')}
FROM
    `{os.environ['GCLOUD_PROJECT']}.{source}.{table}`
"""
create_wprdc_table = BigQueryOperator(
    task_id='create_wprdc_table',
    sql=query_export_cols,
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

# export table as CSV to WPRDC bucket
wprdc_export = BigQueryToCloudStorageOperator(
    task_id='wprdc_export',
    source_project_dataset_table=f"{os.environ['GCLOUD_PROJECT']}.{source}.data_export_wprdc",
    destination_cloud_storage_uris=[f"gs://{os.environ['GCS_PREFIX']}_wprdc/{source}/{dataset}/{export_file}.csv"],
    bigquery_conn_id='google_cloud_default',
    dag=dag
)

# clean up BQ by removing temporary WPRDC table after it has been converted to a CSV
delete_export = BigQueryTableDeleteOperator(
    task_id="delete_export",
    deletion_dataset_table=f"{os.environ['GCLOUD_PROJECT']}.{source}.data_export_wprdc",
    bigquery_conn_id='google_cloud_default',
    dag=dag
)

beam_cleanup = BashOperator(
    task_id='beam_cleanup',
    bash_command=airflow_utils.beam_cleanup_statement(f"{os.environ['GCS_PREFIX']}_{source}"),
    dag=dag
)

tax_delinquency_gcs >> get_coords >> geojoin_council >> geojoin_ward >> geojoin_dpw >> \
    geojoin_police >> geojoin_fire >> insert_new >> update_changed >> create_wprdc_table >> wprdc_export >> \
    delete_export >> beam_cleanup
