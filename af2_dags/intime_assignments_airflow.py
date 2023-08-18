from __future__ import absolute_import

import os
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_month, get_ds_year, get_ds_day, default_args

# The goal of this DAG is to perform a complete pull of police unit assignment data from
# the InTime API. This employee info will be stored in Data Rivers and extracted via PowerShell
# to be merged into the Police Active Directory.

dag = DAG(
    'intime_assignments',
    default_args=default_args,
    schedule_interval='@hourly',
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year,
                          'get_ds_day': get_ds_day},
    max_active_runs=1
)

# initialize gcs locations
dataset = "intime"
json_bucket = f"gs://{os.environ['GCS_PREFIX']}_{dataset}"
hot_bucket = f"gs://{os.environ['GCS_PREFIX']}_hot_metal"
path = "assignments/{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ ds|get_ds_day }}/{{ run_id }}"
json_loc = f"{path}_assignments.json"
avro_loc = "schedule_assignments"

intime_assignments_gcs = BashOperator(
    task_id='intime_assignments_gcs',
    bash_command=f"python {os.environ['GCS_LOADER_PATH']}/intime_assignments_gcs.py --output_arg {json_loc}",
    execution_timeout=timedelta(hours=1),
    dag=dag
)

intime_assignments_dataflow = BashOperator(
    task_id='intime_assignments_dataflow',
    bash_command=f"python {os.environ['dataflow_ETL_PATH']}/intime_assignments_dataflow.py --input {json_loc}",
    dag=dag
)

intime_assignments_bq_load = GoogleCloudStorageToBigQueryOperator(
    task_id='intime_assignments_bq_load',
    destination_project_dataset_table=f"{os.environ['GCLOUD_PROJECT']}.intime.incoming_assignments",
    bucket=f"{os.environ['GCS_PREFIX']}_hot_metal",
    source_objects=[f"{avro_loc}*.avro"],
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    source_format='AVRO',
    autodetect=True,
    bigquery_conn_id='google_cloud_default',
    dag=dag
)

write_append_query = F"""
CREATE OR REPLACE TABLE `{os.environ['GCLOUD_PROJECT']}.intime.schedule_assignments` AS
    SELECT DISTINCT * FROM `{os.environ['GCLOUD_PROJECT']}.intime.schedule_assignments`
    UNION DISTINCT
    SELECT DISTINCT * FROM `{os.environ['GCLOUD_PROJECT']}.intime.incoming_assignments`
"""
write_append_data = BigQueryOperator(
    task_id='write_append_data',
    sql=write_append_query,
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

# Export table to IAPro bucket as readable CSV
# intime_iapro_export = BigQueryToCloudStorageOperator(
#     task_id='intime_iapro_export',
#     source_project_dataset_table=f"{os.environ['GCLOUD_PROJECT']}.{dataset}.employee_data",
#     destination_cloud_storage_uris=[f"gs://{os.environ['GCS_PREFIX']}_iapro/schedule_report.csv"],
#     bigquery_conn_id='google_cloud_default',
#     dag=dag
# )

intime_assignments_gcs >> intime_assignments_dataflow >> intime_assignments_bq_load >> write_append_data
