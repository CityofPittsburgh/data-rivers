from __future__ import absolute_import

import os

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_month, get_ds_year, get_ds_day, default_args

# The goal of this DAG is to perform a daily pull of electric vehicle charging data from
# the Chargepoint API. This charging data will be stored in Data Rivers and displayed on
# the Sustainability & Resilience page of Dashburgh, where it will update dynamically.

dag = DAG(
    'chargepoint',
    default_args=default_args,
    schedule_interval='@daily',
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year,
                          'get_ds_day': get_ds_day}
)

# initialize gcs locations
dataset = "chargepoint"
exec_date = "{{ ds }}"
bucket = f"{os.environ['GCS_PREFIX']}_{dataset}"
path = "energy/{{ ds|get_ds_year }}"#/{{ ds|get_ds_month }}-{{ ds|get_ds_day }}"
json_loc = f"{path}/*_to_{exec_date}_sessions.json"
avro_loc = f"energy/avro_output/{path}"

chargepoint_gcs = BashOperator(
    task_id='chargepoint_gcs',
    bash_command=f"python {os.environ['GCS_LOADER_PATH']}"
                 f"/chargepoint_gcs.py --output_arg {path}",#{json_loc}",
    dag=dag
)

chargepoint_dataflow = BashOperator(
        task_id = 'chargepoint_dataflow',
        bash_command = f"python {os.environ['DATAFLOW_SCRIPT_PATH']}/chargepoint_dataflow.py --input {bucket}/{json_loc} --avro_output {bucket}/{avro_loc}",
        dag = dag
)

chargepoint_bq_load = GoogleCloudStorageToBigQueryOperator(
        task_id = 'chargepoint_bq_load',
        destination_project_dataset_table = f"{os.environ['GCLOUD_PROJECT']}.{dataset}.energy",
        bucket = bucket,
        source_objects = [f"{bucket}/{avro_loc}"],
        write_disposition='WRITE_APPEND',
        create_disposition='CREATE_IF_NEEDED',
        source_format = 'AVRO',
        autodetect = True,
        bigquery_conn_id='google_cloud_default',
        dag = dag
)

beam_cleanup = BashOperator(
        task_id = 'chargepoint_beam_cleanup',
        bash_command = airflow_utils.beam_cleanup_statement(f"{os.environ['GCS_PREFIX']}_chargepoint"),
        dag = dag
)

chargepoint_gcs >> chargepoint_dataflow >> chargepoint_bq_load >> beam_cleanup