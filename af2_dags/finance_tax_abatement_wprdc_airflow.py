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
FINAL_COLS = COLS + ", council_district, ward, public_works_division, ward AS pli_division, police_zone, fire_zone, longitude, latitude"
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
# source = "finance"
# bucket = f"gs://{os.environ['GCS_PREFIX']}_{source}"
# exec_date = "{{ ds }}"
# table = f"property_{dataset}"

data_name = "tax_abatement"
path = "{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ run_id }}"
json_loc = f"{data_name}/{path}_tax_abatement.json"




# export_file = "{{ ds|get_ds_month }}-{{ ds|get_ds_year }}"
# geo_config = [{'geo_table': 'council_districts', 'geo_field': 'council_district'},
#               {'geo_table': 'wards', 'geo_field': 'ward'},
#               {'geo_table': 'dpw_streets_divisions', 'geo_field': 'public_works_division'},
#               {'geo_table': 'police_zones', 'geo_field': 'police_zone'},
#               {'geo_table': 'fire_zones', 'geo_field': 'fire_zone'}]

extract = BashOperator(
    task_id='extract',
    bash_command=f"python {os.environ['GCS_LOADER_PATH']}/finance_tax_abatement_wprdc_extract.py --output_arg"
                 f" {json_loc}",
    dag=dag
)

# the primary key of tax delinquency data is parcel ID; parcel data is also stored in the timebound_geography dataset
# with corresponding geographical boundaries. this query uses the ST_CENTROID geographic function to obtain lat/longs
# for each parcel
query_coords = F"""       
CREATE OR REPLACE TABLE {os.environ['GCLOUD_PROJECT']}.finance.incoming_{data_name} AS
SELECT
    *,
    ST_Y(ST_CENTROID(p.geometry)) AS latitude, 
    ST_X(ST_CENTROID(p.geometry)) AS longitude
FROM `{os.environ['GCLOUD_PROJECT']}.finance.incoming_{data_name}`
LEFT OUTER JOIN `{os.environ['GCLOUD_PROJECT']}.timebound_geography.parcels` p ON pin = p.zone"""

get_coords = BigQueryOperator(
    task_id='get_coords',
    sql=query_coords,
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)


query_geo_join = build_revgeo_time_bound_query('finance', 'incoming_tax_abatement', 'enriched_tax_abatement',
                                               'approved_date', 'pin', 'latitude', 'longitude')
geojoin = BigQueryOperator(
        task_id = 'geojoin',
        sql = query_geo_join,
        bigquery_conn_id='google_cloud_default',
        use_legacy_sql = False,
        dag = dag
)