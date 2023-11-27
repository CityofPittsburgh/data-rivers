from __future__ import absolute_import

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
# from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
# from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
# from airflow.contrib.operators.bigquery_operator import BigQueryOperator


from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_year, get_ds_month, get_ds_day, default_args

# TODO: When Airflow 2.0 is released, upgrade the package, sub in DataFlowPythonOperator for BashOperator,
# and pass the argument 'py_interpreter=python3'

dag = DAG(
    'computronix_solar_panel_gis_wprdc',
    default_args=default_args,
    schedule_interval='@daily',
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year, 'get_ds_day': get_ds_day},
    start_date=datetime(2022, 10, 24),
    catchup = False
)


# initialize gcs locations
bucket = f"gs://{os.environ['GCS_PREFIX']}_computronix"
dataset = "solar_panel_permits"
path = "{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ ds|get_ds_day }}/{{ run_id }}"
json_loc = f"{path}_solar_panel_permits.json"
avro_loc = f"avro_output/{path}/"


# Run gcs_loader
exec_gcs = f"python {os.environ['GCS_LOADER_PATH']}/computronix_solar_panel_permits_gis_wprdc_gcs.py"
gcs_loader = BashOperator(
        task_id = 'gcs_loader',
        bash_command = f"{exec_gcs} --output_arg {dataset}/{json_loc}",
        dag = dag
)


# Run dataflow
exec_df = f"python {os.environ['DATAFLOW_SCRIPT_PATH']}/computronix_solar_panel_permits_gis_wprdc_dataflow.py"
dataflow = BashOperator(
        task_id = 'dataflow',
        bash_command = f"{exec_df} --input {bucket}/{dataset}/{json_loc} --avro_output {bucket}/{dataset}/{avro_loc}",
        dag = dag
)


gcs_loader >> dataflow
