from __future__ import absolute_import

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_month, get_ds_year, get_ds_day, default_args

# This DAG is intended to backfill Ceridian timekeeping data by requesting data from previous years

dag = DAG(
    'ceridian_timekeeping_backfill',
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2023, 8, 23),
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year,
                          'get_ds_day': get_ds_day}
)

# initialize gcs locations
bucket = f"gs://{os.environ['GCS_PREFIX']}_ceridian"
hot_bucket = f"gs://{os.environ['GCS_PREFIX']}_hot_metal"
dataset = "timekeeping"

json_loc = f"{dataset}/2022/2022_timekeeping_backfill.json"
avro_loc = "timekeeping_backfill"

ceridian_timekeeping_backfill_gcs = BashOperator(
    task_id='ceridian_timekeeping_backfill_gcs',
    bash_command=f"python {os.environ['GCS_LOADER_PATH']}/ceridian_timekeeping_backfill_gcs.py --output_arg {json_loc}",
    dag=dag
)

ceridian_timekeeping_backfill_dataflow = BashOperator(
    task_id='ceridian_timekeeping_backfill_dataflow',
    bash_command=f"python {os.environ['DATAFLOW_SCRIPT_PATH']}/ceridian_timekeeping_dataflow.py "
                 f"--input {bucket}/{json_loc} --avro_output {hot_bucket}/{avro_loc}",
    dag=dag
)

ceridian_timekeeping_backfill_bq_load = GoogleCloudStorageToBigQueryOperator(
    task_id='ceridian_timekeeping_backfill_bq_load',
    destination_project_dataset_table=f"{os.environ['GCLOUD_PROJECT']}.ceridian.2022_timesheet_report",
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
CREATE OR REPLACE TABLE `{os.environ['GCLOUD_PROJECT']}.ceridian.historic_timekeeping` AS
    SELECT DISTINCT * FROM `{os.environ['GCLOUD_PROJECT']}.ceridian.historic_timekeeping`
    UNION DISTINCT
    SELECT DISTINCT * FROM `{os.environ['GCLOUD_PROJECT']}.ceridian.2022_timesheet_report`
"""
insert_backfill_data = BigQueryOperator(
    task_id='insert_backfill_data',
    sql=write_append_query,
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

delete_backfill_avro = BashOperator(
    task_id='delete_backfill_avro',
    bash_command=f"gsutil rm -r {hot_bucket}/{avro_loc}*.avro",
    dag=dag
)

beam_cleanup = BashOperator(
    task_id='ceridian_beam_cleanup',
    bash_command=airflow_utils.beam_cleanup_statement(f"{os.environ['GCS_PREFIX']}_ceridian"),
    dag=dag
)

ceridian_timekeeping_backfill_gcs >> ceridian_timekeeping_backfill_dataflow >> ceridian_timekeeping_backfill_bq_load >>\
    insert_backfill_data >> delete_backfill_avro >> beam_cleanup
