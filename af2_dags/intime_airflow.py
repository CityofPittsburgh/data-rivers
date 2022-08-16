from __future__ import absolute_import

import os

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_month, get_ds_year, get_ds_day, default_args

# The goal of this DAG is to perform a daily pull of electric vehicle charging data from
# the intime API. This charging data will be stored in Data Rivers and displayed on
# the Sustainability & Resilience page of Dashburgh, where it will update dynamically.

dag = DAG(
    'intime',
    default_args=default_args,
    schedule_interval='*/15 * * * *',
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year,
                          'get_ds_day': get_ds_day}
)

# initialize gcs locations
dataset = "intime"
bucket = f"gs://{os.environ['GCS_PREFIX']}_{dataset}"
exec_date = "{{ ds }}"
path = "{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ ds|get_ds_day }}/{{ run_id }}"
json_loc = f"{path}_records.json"
avro_loc = f"avro_output/{path}/"

intime_gcs = BashOperator(
    task_id='intime_gcs',
    bash_command=f"python {os.environ['GCS_LOADER_PATH']}/intime_gcs.py --output_arg {json_loc}",
    dag=dag
)

intime_dataflow = BashOperator(
        task_id = 'intime_dataflow',
        bash_command = f"python {os.environ['DATAFLOW_SCRIPT_PATH']}/intime_dataflow.py --input {bucket}/{json_loc} --avro_output {bucket}/{avro_loc}",
        dag = dag
)

intime_bq_load = GoogleCloudStorageToBigQueryOperator(
        task_id = 'intime_bq_load',
        destination_project_dataset_table = f"{os.environ['GCLOUD_PROJECT']}.{dataset}.employee_data",
        bucket = f"{os.environ['GCS_PREFIX']}_intime",
        source_objects = [f"{avro_loc}*.avro"],
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
        source_format = 'AVRO',
        autodetect = True,
        bigquery_conn_id='google_cloud_default',
        dag = dag
)

# Export table as readable CSV to InTime bucket
intime_export = BigQueryToCloudStorageOperator(
        task_id = 'intime_export',
        source_project_dataset_table = f"{os.environ['GCLOUD_PROJECT']}.{dataset}.employee_data",
        destination_cloud_storage_uris = [f"{bucket}/shared/intime_report.csv"],
        bigquery_conn_id='google_cloud_default',
        dag = dag
)

beam_cleanup = BashOperator(
        task_id = 'intime_beam_cleanup',
        bash_command = airflow_utils.beam_cleanup_statement(f"{os.environ['GCS_PREFIX']}_intime"),
        dag = dag
)

intime_gcs >> intime_dataflow >> intime_bq_load >> intime_export >> beam_cleanup