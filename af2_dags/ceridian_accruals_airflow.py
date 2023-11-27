from __future__ import absolute_import

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator

from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_year, get_ds_month, get_ds_day, default_args, gcs_to_email
import dependencies.bq_queries.employee_admin.ceridian_admin as q

# The goal of this DAG is to extract Time Bank accruals from all officers present in the Ceridian system for comparison
# with the time balances found in InTime. The Ceridian figures should be written to InTime in cases where they differ,
# as Dayforce serves as the system of record for accrual balances.

dag = DAG(
    'ceridian_accruals',
    default_args=default_args,
    schedule_interval='00 10 * * 1',
    start_date=datetime(2023, 11, 17),
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year, 'get_ds_day': get_ds_day},
    max_active_runs=1,
    catchup=False
)

bucket = f"gs://{os.environ['GCS_PREFIX']}_ceridian"
dataset = 'accruals'
path = "{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ run_id }}"
json_loc = f"{path}_report.json"
hot_bucket = f"gs://{os.environ['GCS_PREFIX']}_hot_metal"
output_name = f"{dataset}_report"

# Run gcs_loader
accruals_gcs_loader = BashOperator(
    task_id='accruals_gcs_loader',
    bash_command=F"python {os.environ['DAGS_PATH']}/dependencies/gcs_loaders/ceridian_accruals_gcs.py "
                 F"--output_arg {dataset}/{json_loc}",
    dag=dag
)

exec_df = f"python {os.environ['DATAFLOW_SCRIPT_PATH']}/ceridian_accruals_dataflow.py"
accruals_dataflow = BashOperator(
    task_id='ceridian_accruals_dataflow',
    bash_command=f"{exec_df} --input {bucket}/{dataset}/{json_loc} --avro_output {hot_bucket}/{output_name}",
    dag=dag
)

# Load AVRO data produced by dataflow_script into BQ temp table
accruals_gcs_to_bq = GoogleCloudStorageToBigQueryOperator(
    task_id='accruals_gcs_to_bq',
    destination_project_dataset_table=f"{os.environ['GCLOUD_PROJECT']}:ceridian.time_{output_name}",
    bucket=f"{os.environ['GCS_PREFIX']}_hot_metal",
    source_objects=[f"{output_name}*.avro"],
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    source_format='AVRO',
    autodetect=True,
    bigquery_conn_id='google_cloud_default',
    dag=dag
)

compare_timebank_balances = BigQueryOperator(
    task_id='compare_timebank_balances',
    sql=q.compare_timebank_balances(),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

comparison_gcs_export = BigQueryToCloudStorageOperator(
    task_id='comparison_gcs_export',
    source_project_dataset_table=f"{os.environ['GCLOUD_PROJECT']}.ceridian.intime_balance_comparison",
    destination_cloud_storage_uris=[f"gs://{os.environ['GCS_PREFIX']}_ceridian/data_sharing/time_balance_mismatches.csv"],
    bigquery_conn_id='google_cloud_default',
    dag=dag
)

email_comparison = PythonOperator(
    task_id='email_comparison',
    python_callable=gcs_to_email,
    op_kwargs={"bucket": f"{os.environ['GCS_PREFIX']}_ceridian",
               "file_path": "data_sharing/time_balance_mismatches.csv",
               "recipients": ["osar@pittsburghpa.gov"], "cc": [os.environ["EMAIL"]],
               "subject": "ALERT: Time Bank Mismatches",
               "message": "Attached is an extract of all time bank balances that differ between the Ceridian and InTime source systems.",
               "attachment_name": "time_balance_mismatches"},
    dag=dag
)

update_accruals_table = BigQueryOperator(
    task_id='update_accruals_table',
    sql=q.update_time_accruals_table(),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

delete_accruals_avro = BashOperator(
    task_id='delete_accruals_avro',
    bash_command=f"gsutil rm -r {hot_bucket}/{output_name}*.avro",
    dag=dag
)

accruals_beam_cleanup = BashOperator(
    task_id='accruals_beam_cleanup',
    bash_command=airflow_utils.beam_cleanup_statement(f"{os.environ['GCS_PREFIX']}_ceridian"),
    dag=dag
)

accruals_gcs_loader >> accruals_dataflow >> accruals_gcs_to_bq >> compare_timebank_balances >> comparison_gcs_export >>\
    email_comparison >> delete_accruals_avro >> accruals_beam_cleanup
accruals_gcs_loader >> accruals_dataflow >> accruals_gcs_to_bq >> update_accruals_table >> delete_accruals_avro >> \
    accruals_beam_cleanup
