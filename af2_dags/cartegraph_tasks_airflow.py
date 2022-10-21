from __future__ import absolute_import

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_month, get_ds_year, get_ds_day, default_args

# This DAG will perform a daily pull of all work tasks entered into the Cartegraph application

dag = DAG(
    'cartegraph_tasks',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2022, 10, 21),
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year,
                          'get_ds_day': get_ds_day}
)

# initialize gcs locations
bucket = f"gs://{os.environ['GCS_PREFIX']}_cartegraph"
dataset = "tasks"
exec_date = "{{ ds }}"
path = "{{ ds|get_ds_year }}/{{ ds|get_ds_month }}"
json_loc = f"{dataset}/{path}/{exec_date}_tasks.json"
avro_loc = f"{dataset}/avro_output/{path}/" + "{{ run_id }}"

cartegraph_tasks_gcs = BashOperator(
    task_id='cartegraph_gcs',
    bash_command=f"python {os.environ['GCS_LOADER_PATH']}/cartegraph_tasks_gcs.py --output_arg {json_loc}",
    dag=dag
)

cartegraph_tasks_dataflow = BashOperator(
        task_id = 'cartegraph_tasks_dataflow',
        bash_command = f"python {os.environ['DATAFLOW_SCRIPT_PATH']}/cartegraph_tasks_dataflow.py "
                       f"--input {bucket}/{json_loc} --avro_output {bucket}/{avro_loc}",
        dag = dag
)

cartegraph_tasks_bq_load = GoogleCloudStorageToBigQueryOperator(
        task_id = 'cartegraph_tasks_bq_load',
        destination_project_dataset_table = f"{os.environ['GCLOUD_PROJECT']}.cartegraph.tasks",
        bucket = f"{os.environ['GCS_PREFIX']}_cartegraph",
        source_objects = [f"{avro_loc}*.avro"],
        write_disposition='WRITE_APPEND',
        create_disposition='CREATE_IF_NEEDED',
        source_format = 'AVRO',
        autodetect = True,
        bigquery_conn_id='google_cloud_default',
        dag = dag
)

beam_cleanup = BashOperator(
        task_id = 'cartegraph_taks_beam_cleanup',
        bash_command = airflow_utils.beam_cleanup_statement(f"{os.environ['GCS_PREFIX']}_cartegraph_tasks"),
        dag = dag
)

cartegraph_tasks_gcs >> cartegraph_tasks_dataflow >> cartegraph_tasks_bq_load >> beam_cleanup