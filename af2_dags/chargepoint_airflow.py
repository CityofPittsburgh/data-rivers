from __future__ import absolute_import

import os

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_month, get_ds_year, get_ds_day, default_args

COLS_IN_ORDER = """session_id, energy_kwh, start_time_UTC, start_time_EST, start_time_UNIX,
end_time_UTC, end_time_EST, end_time_UNIX, station_id, station_name, port_number, address, city,
state, country, zip, credential_id"""

# The goal of this DAG is to perform a daily pull of electric vehicle charging data from
# the Chargepoint API. This charging data will be stored in Data Rivers and displayed on
# the Sustainability & Resilience page of Dashburgh, where it will update dynamically.

dag = DAG(
    'chargepoint',
    default_args=default_args,
    schedule_interval= '@daily',
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year,
                          'get_ds_day': get_ds_day}
)

# initialize gcs locations
dataset = "chargepoint"
bucket = f"gs://{os.environ['GCS_PREFIX']}_{dataset}"
exec_date = "{{ ds }}"
path = "{{ ds|get_ds_year }}/{{ ds|get_ds_month }}"
json_loc = f"energy/{path}/{exec_date}_sessions.json"
avro_loc = f"energy/avro_output/{path}/" + "{{ run_id }}"

chargepoint_gcs = BashOperator(
    task_id='chargepoint_gcs',
    bash_command=f"python {os.environ['GCS_LOADER_PATH']}/chargepoint_gcs.py --output_arg {json_loc}",
    dag=dag
)

chargepoint_dataflow = BashOperator(
        task_id = 'chargepoint_dataflow',
        bash_command = f"python {os.environ['DATAFLOW_SCRIPT_PATH']}/chargepoint_dataflow.py --input {bucket}/{json_loc} --avro_output {bucket}/{avro_loc}",
        dag = dag
)

chargepoint_bq_load = GoogleCloudStorageToBigQueryOperator(
        task_id = 'chargepoint_bq_load',
        destination_project_dataset_table = f"{os.environ['GCLOUD_PROJECT']}.{dataset}.charging_sessions",
        bucket = f"{os.environ['GCS_PREFIX']}_chargepoint",
        source_objects = [f"{avro_loc}*.avro"],
        write_disposition='WRITE_APPEND',
        create_disposition='CREATE_IF_NEEDED',
        source_format = 'AVRO',
        autodetect = True,
        bigquery_conn_id='google_cloud_default',
        dag = dag
)

# remove duplicate charging sessions and re-create table in correct column order
query_format_dedupe = f"""
CREATE OR REPLACE TABLE `{os.environ['GCLOUD_PROJECT']}.chargepoint.charging_sessions` AS
WITH formatted  AS 
    (
    SELECT  DISTINCT * FROM 
        {os.environ['GCLOUD_PROJECT']}.chargepoint.charging_sessions
    )
SELECT 
    {COLS_IN_ORDER} 
FROM 
    formatted
"""
chargepoint_format_dedupe = BigQueryOperator(
        task_id = 'chargepoint_format_dedupe',
        sql = query_format_dedupe,
        bigquery_conn_id='google_cloud_default',
        use_legacy_sql = False,
        dag = dag
)

beam_cleanup = BashOperator(
        task_id = 'chargepoint_beam_cleanup',
        bash_command = airflow_utils.beam_cleanup_statement(f"{os.environ['GCS_PREFIX']}_chargepoint"),
        dag = dag
)

chargepoint_gcs >> chargepoint_dataflow >> chargepoint_bq_load >> chargepoint_format_dedupe >> beam_cleanup