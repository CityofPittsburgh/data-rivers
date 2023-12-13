from __future__ import absolute_import

import os
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_table_delete_operator import BigQueryTableDeleteOperator
from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_month, get_ds_year, get_ds_day, default_args

import dependencies.bq_queries.general_queries as q

dag = DAG(
    'time_balance_backfill',
    default_args=default_args,
    schedule_interval=None,
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year,
                          'get_ds_day': get_ds_day},
    max_active_runs=1
)

exec_date = "{{ ds }}"
c_bucket = f"gs://{os.environ['GCS_PREFIX']}_ceridian"
c_path = "accruals/backfill/{{ ds|get_ds_year }}"
c_loc = f"{c_path}/{exec_date}_backfill.json"
i_bucket = f"gs://{os.environ['GCS_PREFIX']}_intime"
i_path = "timebank/backfill/{{ ds|get_ds_year }}"
i_loc = f"{i_path}/{exec_date}_backfill.json"
hot_bucket = f"gs://{os.environ['GCS_PREFIX']}_hot_metal"
output = "backfilled_"

time_balance_backfill_gcs = BashOperator(
    task_id='time_balance_backfill_gcs',
    bash_command=f"python {os.environ['GCS_LOADER_PATH']}/time_balance_backfill_gcs.py --ceridian_output {c_loc} "
                 f"--intime_output {i_loc}",
    dag=dag
)

c_exec_df = f"python {os.environ['DATAFLOW_SCRIPT_PATH']}/ceridian_accruals_dataflow.py"
accruals_backfill_dataflow = BashOperator(
    task_id='accruals_backfill_dataflow',
    bash_command=f"{c_exec_df} --input {c_bucket}/{c_loc} --avro_output {hot_bucket}/{output}accruals "
                 f"--specify_runner DataflowRunner",
    dag=dag
)

accruals_backfill_to_bq = GoogleCloudStorageToBigQueryOperator(
    task_id='accruals_backfill_to_bq',
    destination_project_dataset_table=f"{os.environ['GCLOUD_PROJECT']}:ceridian.accruals_backfill_{exec_date}",
    bucket=f"{os.environ['GCS_PREFIX']}_hot_metal",
    source_objects=[f"{output}accruals*.avro"],
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    source_format='AVRO',
    autodetect=True,
    bigquery_conn_id='google_cloud_default',
    dag=dag
)

backfill_to_accruals_table = BigQueryOperator(
    task_id='backfill_to_accruals_table',
    sql=q.update_time_balances_table('ceridian', 'historic_accrual_balances', f'accruals_backfill_{exec_date}'),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

delete_accruals_backfill = BigQueryTableDeleteOperator(
    task_id="delete_accruals_backfill",
    deletion_dataset_table=f"{os.environ['GCLOUD_PROJECT']}.ceridian.accruals_backfill_{exec_date}",
    bigquery_conn_id='google_cloud_default',
    dag=dag
)

i_exec_df = f"python {os.environ['DATAFLOW_SCRIPT_PATH']}/intime_timebank_dataflow.py"
timebank_backfill_dataflow = BashOperator(
    task_id='timebank_backfill_dataflow',
    bash_command=f"{i_exec_df} --input {i_bucket}/{i_loc} --avro_output {hot_bucket}/{output}timebanks "
                 f"--specify_runner DataflowRunner",
    dag=dag
)

timebank_backfill_to_bq = GoogleCloudStorageToBigQueryOperator(
    task_id='timebank_backfill_to_bq',
    destination_project_dataset_table=f"{os.environ['GCLOUD_PROJECT']}:intime.timebank_backfill_{exec_date}",
    bucket=f"{os.environ['GCS_PREFIX']}_hot_metal",
    source_objects=[f"{output}timebanks*.avro"],
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    source_format='AVRO',
    autodetect=True,
    bigquery_conn_id='google_cloud_default',
    dag=dag
)

backfill_to_timebank_table = BigQueryOperator(
    task_id='backfill_to_timebank_table',
    sql=q.update_time_balances_table('intime', 'timebank_balances', f'timebank_backfill_{exec_date}'),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

delete_timebank_backfill = BigQueryTableDeleteOperator(
    task_id="delete_timebank_backfill",
    deletion_dataset_table=f"{os.environ['GCLOUD_PROJECT']}.intime.timebank_backfill_{exec_date}",
    bigquery_conn_id='google_cloud_default',
    dag=dag
)


time_balance_backfill_gcs >> accruals_backfill_dataflow >> accruals_backfill_to_bq >> backfill_to_accruals_table >> \
    delete_accruals_backfill
time_balance_backfill_gcs >> timebank_backfill_dataflow >> timebank_backfill_to_bq >> backfill_to_timebank_table >> \
    delete_timebank_backfill
