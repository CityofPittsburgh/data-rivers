from __future__ import absolute_import

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_month, get_ds_year, get_ds_day, default_args, \
    build_insert_new_records_query

# The goal of this DAG is to perform a monthly pull of timesheet entries for all
# City of Pittsburgh employee via the Ceridian Dayforce API. This  data will be stored securely
# in Data Rivers and visualized in a Looker Studio dashboard for internal usage.

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
hot_bucket = f"gs://{os.environ['GCS_PREFIX']}_hot_metal"
dataset = "timekeeping"

# create one single json file per month that gets overwritten each day
today = datetime.today()
first_curr_month = today.replace(day=1)
prev_month_last = first_curr_month - timedelta(days=1)
prev_month = str(prev_month_last.date().strftime("%m-%Y"))
path = "{{ ds|get_ds_year }}"

json_loc = f"{dataset}/{path}/{prev_month}_timekeeping.json"
avro_loc = "employee_timekeeping"

ceridian_gcs = BashOperator(
    task_id='ceridian_timekeeping_gcs',
    bash_command=f"python {os.environ['GCS_LOADER_PATH']}/ceridian_timekeeping_gcs.py --output_arg {json_loc}",
    dag=dag
)

ceridian_dataflow = BashOperator(
    task_id='ceridian_timekeeping_dataflow',
    bash_command=f"python {os.environ['DATAFLOW_SCRIPT_PATH']}/ceridian_timekeeping_dataflow.py "
                 f"--input {bucket}/{json_loc} --avro_output {hot_bucket}/{avro_loc}",
    dag=dag
)

ceridian_bq_load = GoogleCloudStorageToBigQueryOperator(
    task_id='ceridian_timekeeping_bq_load',
    destination_project_dataset_table=f"{os.environ['GCLOUD_PROJECT']}.ceridian.monthly_timesheet_report",
    bucket=hot_bucket,
    source_objects=[f"{avro_loc}*.avro"],
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    source_format='AVRO',
    autodetect=True,
    bigquery_conn_id='google_cloud_default',
    dag=dag
)

write_append_query = F"""
CREATE OR REPLACE TABLE `{os.environ['GCLOUD_PROJECT']}.ceridian.historic_timekeeping` AS
    SELECT * FROM `{os.environ['GCLOUD_PROJECT']}.ceridian.historic_timekeeping`
    UNION DISTINCT
    SELECT * FROM `{os.environ['GCLOUD_PROJECT']}.ceridian.monthly_timesheet_report`
"""
insert_monthly_data = BigQueryOperator(
    task_id='insert_monthly_data',
    sql=write_append_query,
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

delete_avro = BashOperator(
    task_id='delete_avro',
    bash_command=f"gsutil rm -r gs://{hot_bucket}/{avro_loc}*.avro",
    dag=dag
)

beam_cleanup = BashOperator(
    task_id='ceridian_beam_cleanup',
    bash_command=airflow_utils.beam_cleanup_statement(f"{os.environ['GCS_PREFIX']}_ceridian"),
    dag=dag
)

ceridian_gcs >> ceridian_dataflow >> ceridian_bq_load >> insert_monthly_data >> delete_avro >> beam_cleanup
