from __future__ import absolute_import

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_month, get_ds_year, get_ds_day, default_args

COLS_IN_ORDER = """"""

# This DAG will perform a daily pull of all open Cherwell Service Request tickets

dag = DAG(
    'cherwell_incidents',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2023, 2, 6),
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year,
                          'get_ds_day': get_ds_day}
)

# initialize gcs locations
bucket = f"gs://{os.environ['GCS_PREFIX']}_cherwell"
dataset = "incidents"
exec_date = "{{ ds }}"
path = "{{ ds|get_ds_year }}/{{ ds|get_ds_month }}"
json_loc = f"{dataset}/{path}/{exec_date}_incidents.json"
avro_loc = f"{dataset}/avro_output/{path}/" + "{{ run_id }}"

cherwell_incidents_gcs = BashOperator(
    task_id='cherwell_incidents_gcs',
    bash_command=f"python {os.environ['GCS_LOADER_PATH']}/cherwell_incidents_gcs.py --output_arg {json_loc}",
    dag=dag
)

cherwell_incidents_gcs