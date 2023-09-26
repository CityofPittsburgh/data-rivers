from __future__ import absolute_import

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_month, get_ds_year, get_ds_day, default_args

# This DAG performs a daily extract of users from the City's Active Directory domain and fills in/corrects malformed
# or missing data using data taken in from the Ceridian and InTime data pipelines. The end goal is to ensure that
# all City employees have accounts that are set up with the proper credentials to access the City network as close
# to account creation as possible.

dag = DAG(
    'active_directory_users',
    default_args=default_args,
    schedule_interval=None,  # '@daily',
    start_date=datetime(2023, 9, 25),
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year,
                          'get_ds_day': get_ds_day}
)

# initialize gcs locations
json_bucket = f"gs://{os.environ['GCS_PREFIX']}_active_directory"
dataset = "users"
exec_date = "{{ ds }}"
path = "{{ ds|get_ds_year }}/{{ ds|get_ds_month }}"
json_loc = f"{dataset}/{path}/{exec_date}_users.json"
hot_bucket = f"gs://{os.environ['GCS_PREFIX']}_hot_metal"

active_directory_users_gcs = BashOperator(
    task_id='active_directory_users_gcs',
    bash_command=f"python {os.environ['GCS_LOADER_PATH']}/active_directory_users_gcs.py --output_arg {json_loc}",
    dag=dag
)

active_directory_users_dataflow = BashOperator(
    task_id='active_directory_users_dataflow',
    bash_command=f"python {os.environ['DATAFLOW_SCRIPT_PATH']}/active_directory_users_dataflow.py "
                 f"--input {json_bucket}/{json_loc} --avro_output {hot_bucket}/ad_{dataset}",
    dag=dag
)

active_directory_users_bq_load = GoogleCloudStorageToBigQueryOperator(
    task_id='active_directory_users_bq_load',
    destination_project_dataset_table=f"{os.environ['GCLOUD_PROJECT']}.active_directory.ad_users_raw",
    bucket=f"{os.environ['GCS_PREFIX']}_hot_metal",
    source_objects=[f"ad_{dataset}*.avro"],
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    source_format='AVRO',
    autodetect=True,
    bigquery_conn_id='google_cloud_default',
    dag=dag
)

active_directory_users_gcs >> active_directory_users_dataflow >> active_directory_users_bq_load
