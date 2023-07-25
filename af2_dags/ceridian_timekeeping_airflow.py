from __future__ import absolute_import

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_month, get_ds_year, get_ds_day, \
    default_args, build_percentage_table_query

# The goal of this DAG is to perform a daily pull of basic demographic information for each
# City of Pittsburgh employee via the Ceridian Dayforce API. This  data will be stored securely
# in Data Rivers and used for a few projects. For one, we will use the list of timekeeping returned
# by the API to determine who has left the City payroll so that we can stop keeping track of
# their COVID vaccination status. Additionally, we will use de-identified race, sex, and union
# membership totals to display on Dashburgh. This will give the public insight on the demographics
# of the city government and how it compares to the demographics of the city as a whole.

dag = DAG(
    'ceridian_timekeeping',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2023, 7, 24),
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year,
                          'get_ds_day': get_ds_day}
)

# initialize gcs locations
bucket = f"gs://{os.environ['GCS_PREFIX']}_ceridian"
dataset = "timekeeping"
# path = "{{ ds + macros.dateutil.relativedelta.relativedelta(months=-1, day=1) }}"
today = datetime.today()
first_curr_month = today.replace(day=1)
prev_month_last = first_curr_month - timedelta(days=1)
prev_month = str(prev_month_last.date().strftime("%m-%Y"))
path = "{{ ds|get_ds_year }}"

json_loc = f"{dataset}/{path}/{prev_month}_timekeeping.json"
avro_loc = f"{dataset}/avro_output/{path}/" + "{{ run_id }}"

ceridian_gcs = BashOperator(
    task_id='ceridian_gcs',
    bash_command=f"python {os.environ['GCS_LOADER_PATH']}/ceridian_timekeeping_gcs.py --output_arg {json_loc}",
    dag=dag
)

ceridian_dataflow = BashOperator(
    task_id='ceridian_dataflow',
    bash_command=f"python {os.environ['DATAFLOW_SCRIPT_PATH']}/ceridian_timekeeping_dataflow.py --input {bucket}/{json_loc} "
                 f"--avro_output {bucket}/{avro_loc}",
    dag=dag
)

ceridian_bq_load = GoogleCloudStorageToBigQueryOperator(
    task_id='ceridian_bq_load',
    destination_project_dataset_table=f"{os.environ['GCLOUD_PROJECT']}.ceridian.monthly_timesheet_report",
    bucket=f"{os.environ['GCS_PREFIX']}_ceridian",
    source_objects=[f"{avro_loc}*.avro"],
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    source_format='AVRO',
    autodetect=True,
    bigquery_conn_id='google_cloud_default',
    dag=dag
)

beam_cleanup = BashOperator(
    task_id='ceridian_beam_cleanup',
    bash_command=airflow_utils.beam_cleanup_statement(f"{os.environ['GCS_PREFIX']}_ceridian"),
    dag=dag
)

ceridian_gcs >> ceridian_dataflow >> ceridian_bq_load >> beam_cleanup
