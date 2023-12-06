from __future__ import absolute_import

import os
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator

from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_month, get_ds_year, get_ds_day, default_args, gcs_to_email
import dependencies.bq_queries.employee_admin.ceridian_admin as q

dag = DAG(
    'intime_set_balances',
    default_args=default_args,
    schedule_interval=None,
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year,
                          'get_ds_day': get_ds_day},
    max_active_runs=1
)

exec_date = "{{ ds }}"
path = "timebank/update_log/{{ ds|get_ds_year }}/{{ ds|get_ds_month }}"
json_loc = f"{path}/{exec_date}_updates.json"

create_discrepancy_table = BigQueryOperator(
    task_id='create_discrepancy_table',
    sql=q.compare_timebank_balances('ceridian', 'discrepancy_report', -2),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

comparison_gcs_export = BigQueryToCloudStorageOperator(
    task_id='comparison_gcs_export',
    source_project_dataset_table=f"{os.environ['GCLOUD_PROJECT']}.ceridian.discrepancy_report",
    destination_cloud_storage_uris=[f"gs://{os.environ['GCS_PREFIX']}_ceridian/data_sharing/discrepancy_report.csv"],
    bigquery_conn_id='google_cloud_default',
    dag=dag
)

email_comparison = PythonOperator(
    task_id='email_comparison',
    python_callable=gcs_to_email,
    op_kwargs={"bucket": f"{os.environ['GCS_PREFIX']}_ceridian",
               "file_path": "data_sharing/discrepancy_report.csv",
               "recipients": ["osar@pittsburghpa.gov"], "cc": [os.environ["EMAIL"], "benjamin.cogan@pittsburghpa.gov"],
               "subject": "ALERT: Time Bank Discrepancy Report",
               "message": "Attached is an extract of all time bank balances that differ between the Ceridian and InTime source systems.",
               "attachment_name": "discrepancy_report"},
    dag=dag
)

create_update_table = BigQueryOperator(
    task_id='create_update_table',
    sql=q.compare_timebank_balances('intime', 'balance_update', -14),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

export_update_table = BigQueryToCloudStorageOperator(
    task_id='export_update_table',
    source_project_dataset_table=f"{os.environ['GCLOUD_PROJECT']}.intime.balance_update",
    destination_cloud_storage_uris=[f"gs://{os.environ['GCS_PREFIX']}_intime/timebank/time_balance_mismatches.csv"],
    bigquery_conn_id='google_cloud_default',
    dag=dag
)


set_balances_gcs = BashOperator(
    task_id='set_balances_gcs',
    bash_command=f"python {os.environ['GCS_LOADER_PATH']}/intime_set_balances_gcs.py --output_arg {json_loc}",
    dag=dag
)

create_discrepancy_table >> comparison_gcs_export >> email_comparison
create_update_table >> export_update_table >> set_balances_gcs
