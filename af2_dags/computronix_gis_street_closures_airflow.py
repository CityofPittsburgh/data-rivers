from __future__ import absolute_import

import os

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator

from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_year, get_ds_month, get_ds_day, default_args

# TODO: When Airflow 2.0 is released, upgrade the package, sub in DataFlowPythonOperator for BashOperator,
# and pass the argument 'py_interpreter=python3'

dag = DAG(
    'computronix_gis_street_closures',
    default_args=default_args,
    schedule_interval='@daily',
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year, 'get_ds_day': get_ds_day}
)


# initialize gcs locations
bucket = f"gs://{os.environ['GCS_PREFIX']}_computronix"
dataset = "gis_domi_street_closures"
path = "{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ ds|get_ds_day }}/{{ run_id }}"
json_loc = f"{path}_street_closures.json"
avro_loc = f"avro_output/{path}/"


# Run gcs_loader
exec_gcs = f"python {os.environ['GCS_LOADER_PATH']}/computronix_gis_street_closures_gcs.py"
gcs_loader = BashOperator(
        task_id = 'gcs_loader',
        bash_command = f"{exec_gcs} --output_arg {dataset}/{json_loc}",
        dag = dag
)


exec_df = f"python {os.environ['DATAFLOW_SCRIPT_PATH']}/computronix_gis_street_closures_dataflow.py"
dataflow = BashOperator(
        task_id = 'dataflow',
        bash_command = f"{exec_df} --input {bucket}/{dataset}/{json_loc} --avro_output {bucket}/{dataset}/{avro_loc}",
        dag = dag
)


# Load AVRO data produced by dataflow_script into BQ temp table
gcs_to_bq = GoogleCloudStorageToBigQueryOperator(
        task_id = 'gcs_to_bq',
        destination_project_dataset_table = f"{os.environ['GCLOUD_PROJECT']}:computronix.gis_street_closures",
        bucket = f"{os.environ['GCS_PREFIX']}_computronix",
        source_objects = [f"{dataset}/{avro_loc}*.avro"],
        write_disposition = 'WRITE_TRUNCATE',
        create_disposition = 'CREATE_IF_NEEDED',
        source_format = 'AVRO',
        autodetect = True,
        dag = dag
)


# Export table as CSV to WPRDC bucket
# file name is the date. path contains the date info
csv_file_name = f"{path}"
dest_bucket = f"gs://{os.environ['GCS_PREFIX']}_gis_team/domi_street_closure_segments/"
gis_export = BigQueryToCloudStorageOperator(
        task_id = 'gis_export',
        source_project_dataset_table = f"{os.environ['GCLOUD_PROJECT']}.computronix.gis_street_closures",
        destination_cloud_storage_uris = [f"{dest_bucket}/{csv_file_name}.csv"],
        dag = dag
)


beam_cleanup = BashOperator(
    task_id='beam_cleanup',
    bash_command=airflow_utils.beam_cleanup_statement('{}_computronix'.format(os.environ['GCS_PREFIX'])),
    dag=dag
)

gcs_loader >> dataflow >> gcs_to_bq >> gis_export >> beam_cleanup

