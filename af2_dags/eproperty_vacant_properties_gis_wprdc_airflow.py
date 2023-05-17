from __future__ import absolute_import

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_year, get_ds_month, get_ds_day, default_args, \
    build_revgeo_time_bound_query

# TODO: When Airflow 2.0 is released, upgrade the package, sub in DataFlowPythonOperator for BashOperator,
# and pass the argument 'py_interpreter=python3'

dag = DAG(
        'eprop_vacant_gis_wprdc',
        default_args = default_args,
        schedule_interval = '@daily',
        user_defined_filters = {'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year, 'get_ds_day': get_ds_day},
        start_date = datetime(2023, 5, 1),
        catchup = False
)

# initialize gcs locations
bucket = f"gs://{os.environ['GCS_PREFIX']}_eproperty"
path = "{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ ds|get_ds_day }}/{{ run_id }}"
json_loc = "{{ ds|get_ds_month }}/{{ ds|get_ds_day }}" + "_vacant_properties.json"

# Run gcs_loader
exec_gcs = f"python {os.environ['GCS_LOADER_PATH']}/eprop_vacant_properties_gis_wprdc_gcs.py"
gcs_loader = BashOperator(
        task_id = 'gcs_loader',
        bash_command = f"{exec_gcs} --json_output_arg {json_loc}",
        dag = dag
)


# reverse geocode
query_geo_join = build_revgeo_time_bound_query('eproperty', 'vacant_properties', 'vacant_properties_enriched',
                                               'status_date_utc', 'id', 'lat', 'long')
geojoin = BigQueryOperator(
        task_id = 'geojoin',
        sql = query_geo_join,
        bigquery_conn_id = 'google_cloud_default',
        use_legacy_sql = False,
        dag = dag
)


# Export table as CSV to WPRDC bucket
# file name is the date. path contains the date info
csv_file_name = f"{path}"
dest_bucket = f"gs://{os.environ['GCS_PREFIX']}_wprdc/eproperty/vacant_properties/"
wprdc_export = BigQueryToCloudStorageOperator(
        task_id = 'wprdc_export',
        source_project_dataset_table = f"{os.environ['GCLOUD_PROJECT']}.eproperty.vacant_properties",
        destination_cloud_storage_uris = [f"{dest_bucket}{csv_file_name}.csv"],
        dag = dag
)


# push table of ALL properties data-bridGIS BQ
query_push_gis = F"""
CREATE OR REPLACE TABLE `data-bridgis.eproperty.gis_vacant_properties_enriched` AS 
SELECT 
  *
FROM `{os.environ["GCLOUD_PROJECT"]}.eproperty.vacant_properties_enriched`
"""
push_gis = BigQueryOperator(
        task_id = 'push_gis',
        sql = query_push_gis,
        bigquery_conn_id = 'google_cloud_default',
        use_legacy_sql = False,
        dag = dag
)


beam_cleanup = BashOperator(
        task_id = 'beam_cleanup',
        bash_command = airflow_utils.beam_cleanup_statement(f"{os.environ['GCS_PREFIX']}_eproperties"),
        dag = dag
)


gcs_loader >> geojoin
geojoin >> wprdc_export >> beam_cleanup
geojoin >> push_gis >> beam_cleanup
