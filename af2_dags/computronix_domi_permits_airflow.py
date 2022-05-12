from __future__ import absolute_import

import os

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_year, get_ds_month, get_ds_day, default_args, build_revgeo_query

# TODO: When Airflow 2.0 is released, upgrade the package, sub in DataFlowPythonOperator for BashOperator,
# and pass the argument 'py_interpreter=python3'


dag = DAG(
    'computronix_domi_permits',
    default_args=default_args,
    schedule_interval='@hourly',
    user_defined_filters = {'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year,
                            'get_ds_day'  : get_ds_day}
)


# initialize GCS and BQ locations
bucket = f"gs://{os.environ['GCS_PREFIX']}_computronix"
dataset = "domi_permits"
path = "{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ ds|get_ds_day }}/{{ run_id }}"
json_loc = f"{path}_permits.json"
avro_loc = f"avro_output/{path}/"


# Run GCS loader
exec_gcs = f"python {os.environ['GCS_LOADER_PATH']}/computronix_domi_permits_gcs.py"
computronix_domi_permits_gcs = BashOperator(
    task_id='computronix_domi_permits_gcs',
    bash_command= f"{exec_gcs} --output_arg {dataset}/{json_loc}",
    dag=dag
)


# Run DF script
exec_df = f"python {os.environ['DATAFLOW_SCRIPT_PATH']}/computronix_domi_permits_dataflow.py"
computronix_domi_permits_dataflow = BashOperator(
    task_id='computronix_domi_permits_dataflow',
    bash_command= f"{exec_df} --input {bucket}/{dataset}/{json_loc} --avro_output {bucket}/{dataset}/{avro_loc}",
    dag=dag
)

computronix_domi_permits_bq = GoogleCloudStorageToBigQueryOperator(
    task_id='computronix_domi_permits_bq',
    destination_project_dataset_table='{}:computronix.domi_permits_raw'.format(os.environ['GCLOUD_PROJECT']),
    bucket='{}_computronix'.format(os.environ['GCS_PREFIX']),
    source_objects=["domi_permits/avro_output/{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ ds }}/*.avro"],
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    time_partitioning={'type': 'DAY'},
    source_format='AVRO',
    autodetect=True,
    dag=dag
)

computronix_domi_permits_geojoin = BigQueryOperator(
    task_id='computronix_domi_permits_geojoin',
    sql=build_revgeo_query('computronix', 'domi_permits_raw', 'JOBID'),
    use_legacy_sql=False,
    destination_dataset_table='{}:computronix.domi_permits'.format(os.environ['GCLOUD_PROJECT']),
    write_disposition='WRITE_TRUNCATE',
    time_partitioning={'type': 'DAY'},
    dag=dag
)

computronix_domi_permits_beam_cleanup = BashOperator(
    task_id='computronix_domi_permits_beam_cleanup',
    bash_command= airflow_utils.beam_cleanup_statement('{}_computronix'.format(os.environ['GCS_PREFIX'])),
    dag=dag
)

computronix_domi_permits_gcs >> computronix_domi_permits_dataflow >> computronix_domi_permits_bq >> \
    (computronix_domi_permits_geojoin, computronix_domi_permits_beam_cleanup)
