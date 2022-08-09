from __future__ import absolute_import

import os

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_month, get_ds_year, get_ds_day, default_args

COLS_IN_ORDER = "employee_id, first_name, last_name, email, rank, unit"

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

# remove duplicate charging sessions and re-create table in correct column order
query_format_dedupe = f"""
CREATE OR REPLACE TABLE `{os.environ['GCLOUD_PROJECT']}.intime.employee_data` AS
WITH formatted  AS 
    (
    SELECT  DISTINCT * FROM 
        {os.environ['GCLOUD_PROJECT']}.intime.employee_data
    )
SELECT 
    {COLS_IN_ORDER} 
FROM 
    formatted
"""
intime_format_dedupe = BigQueryOperator(
        task_id = 'intime_format_dedupe',
        sql = query_format_dedupe,
        bigquery_conn_id='google_cloud_default',
        use_legacy_sql = False,
        dag = dag
)

beam_cleanup = BashOperator(
        task_id = 'intime_beam_cleanup',
        bash_command = airflow_utils.beam_cleanup_statement(f"{os.environ['GCS_PREFIX']}_intime"),
        dag = dag
)

intime_gcs >> intime_dataflow >> intime_bq_load >> intime_format_dedupe >> beam_cleanup