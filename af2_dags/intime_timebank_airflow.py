from __future__ import absolute_import

import os

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_year, get_ds_month, get_ds_day, default_args
import dependencies.bq_queries.employee_admin.intime_admin as q

# The goal of this DAG is to extract time bank balances from all officers present in the InTime system for comparison
# with the time accruals found in Ceridian. The Ceridian figures should be written to InTime in cases where they differ.

dag = DAG(
    'intime_timebank',
    default_args=default_args,
    schedule_interval=None,
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year, 'get_ds_day': get_ds_day},
    max_active_runs=1,
    catchup=False
)

bucket = f"gs://{os.environ['GCS_PREFIX']}_intime"
dataset = 'timebank'
table = 'balances'
path = "{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ run_id }}"
json_loc = f"{path}_{table}.json"
hot_bucket = f"gs://{os.environ['GCS_PREFIX']}_hot_metal"
output_name = f"{dataset}_{table}"

# Run gcs_loader
timebank_gcs_loader = BashOperator(
    task_id='timebank_gcs_loader',
    bash_command=F"python {os.environ['DAGS_PATH']}/dependencies/gcs_loaders/intime_timebank_gcs.py "
                 F"--output_arg {dataset}/{json_loc}",
    dag=dag
)

exec_df = f"python {os.environ['DATAFLOW_SCRIPT_PATH']}/intime_timebank_dataflow.py"
timebank_dataflow = BashOperator(
    task_id='intime_timebank_dataflow',
    bash_command=f"{exec_df} --input {bucket}/{dataset}/{json_loc} --avro_output {hot_bucket}/{output_name}",
    dag=dag
)

# Load AVRO data produced by dataflow_script into BQ temp table
timebank_gcs_to_bq = GoogleCloudStorageToBigQueryOperator(
    task_id='timebank_gcs_to_bq',
    destination_project_dataset_table=f"{os.environ['GCLOUD_PROJECT']}:intime.weekly_time_balances",
    bucket=f"{os.environ['GCS_PREFIX']}_hot_metal",
    source_objects=[f"{output_name}*.avro"],
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    source_format='AVRO',
    autodetect=True,
    bigquery_conn_id='google_cloud_default',
    dag=dag
)

update_balances_table = BigQueryOperator(
    task_id='update_balances_table',
    sql=q.update_timebank_table(),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

delete_timebank_avro = BashOperator(
    task_id='delete_timebank_avro',
    bash_command=f"gsutil rm -r {hot_bucket}/{output_name}*.avro",
    dag=dag
)

timebank_beam_cleanup = BashOperator(
    task_id='timebank_beam_cleanup',
    bash_command=airflow_utils.beam_cleanup_statement(f"{os.environ['GCS_PREFIX']}_intime"),
    dag=dag
)

timebank_gcs_loader >> timebank_dataflow >> timebank_gcs_to_bq >> update_balances_table >> delete_timebank_avro >> \
    timebank_beam_cleanup
