from __future__ import absolute_import

import os
import pendulum
import pytz

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_month, get_ds_year, get_ds_day, default_args

from dependencies.bq_queries.ceridian import employee_admin as q

# This is a small DAG that will run every morning and extract a subset of the ceridian.all_employees table
# that includes all employees that were hired within the past day and store the query results in a CSV in Cloud
# Storage. This CSV will then be migrated to a Sharepoint folder that will be made viewable by managers.

dataset = 'ceridian'
dir = 'new_hires'
exec_date = "{{ ds }}"
path = "{{ ds|get_ds_year }}/{{ ds|get_ds_month }}"
csv_loc = f"{exec_date}_new_hire_report.csv"

dag = DAG(
    'ceridian_new_hires',
    default_args=default_args,
    schedule_interval='0 6 * * *',
    start_date=pendulum.datetime(2023, 8, 14, 0, 6, tz=pytz.timezone('US/Eastern')),
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year,
                          'get_ds_day': get_ds_day}
)

# union all  new records with the existing records that did not get pulled in if an api request error occurred
extract_new_hires = BigQueryOperator(
    task_id='extract_new_hires',
    sql=q.extract_new_hires(),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

new_hires_to_csv = BigQueryToCloudStorageOperator(
    task_id='new_hires_to_csv',
    source_project_dataset_table=f"{os.environ['GCLOUD_PROJECT']}.{dataset}.daily_{dir}",
    destination_cloud_storage_uris=[f"gs://{os.environ['GCS_PREFIX']}_{dataset}/{dir}/{path}/{csv_loc}"],
    bigquery_conn_id='google_cloud_default',
    dag=dag
)

new_hires_etl = BashOperator(
    task_id='new_hires_etl',
    bash_command=f"python {os.environ['PANDAS_ETL_PATH']}/ceridian_new_hires_etl.py "
                 f"--gcs_input {dir}/{path}/{csv_loc} --sharepoint_subdir {path}/ --sharepoint_output {csv_loc}",
    dag=dag
)


extract_new_hires >> new_hires_to_csv >> new_hires_etl
