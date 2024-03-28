from __future__ import absolute_import

import os
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_month, get_ds_year, get_ds_day, default_args
from dependencies.bq_queries.employee_admin import intime_admin as q

# The goal of this DAG is to perform a complete pull of police unit assignment data from
# the InTime API. This employee info will be stored in Data Rivers and extracted via PowerShell
# to be merged into the Police Active Directory.

dag = DAG(
    'intime_employees',
    default_args=default_args,
    schedule_interval='@hourly',
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year,
                          'get_ds_day': get_ds_day},
    max_active_runs=1
)

# initialize gcs locations
dataset = "intime"
bucket = f"gs://{os.environ['GCS_PREFIX']}_{dataset}"
path = "employees/{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ ds|get_ds_day }}/{{ run_id }}"
json_loc = f"{path}_records.json"

intime_employees_gcs = BashOperator(
    task_id='intime_employees_gcs',
    bash_command=f"python {os.environ['GCS_LOADER_PATH']}/intime_employees_gcs.py --output_arg {json_loc}",
    execution_timeout=timedelta(hours=1),
    dag=dag
)

intime_employees_pandas = BashOperator(
    task_id='intime_employees_pandas',
    bash_command=f"python {os.environ['PANDAS_ETL_PATH']}/intime_employees_pandas.py --input {json_loc}",
    dag=dag
)

active_officers_export = BigQueryOperator(
    task_id='active_officers_export',
    sql=q.export_active_officers(),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

# Export table to IAPro bucket as readable CSV
intime_iapro_export = BigQueryToCloudStorageOperator(
    task_id='intime_iapro_export',
    source_project_dataset_table=f"{os.environ['GCLOUD_PROJECT']}.{dataset}.active_employees",
    destination_cloud_storage_uris=[f"gs://{os.environ['GCS_PREFIX']}_iapro/intime_report.csv"],
    bigquery_conn_id='google_cloud_default',
    dag=dag
)

intime_employees_gcs >> intime_employees_pandas >> active_officers_export >> intime_iapro_export
