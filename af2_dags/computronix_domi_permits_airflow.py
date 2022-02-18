from __future__ import absolute_import

import os

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_year, get_ds_month, default_args, build_revgeo_query

# TODO: When Airflow 2.0 is released, upgrade the package, sub in DataFlowPythonOperator for BashOperator,
# and pass the argument 'py_interpreter=python3'

dag = DAG(
    'computronix_domi_permits',
    default_args=default_args,
    schedule_interval='@daily',
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year}
)

computronix_domi_permits_gcs = BashOperator(
    task_id='computronix_domi_permits_gcs',
    bash_command="python {}".format(os.environ['DAGS_PATH']) + "/dependencies/gcs_loaders"
                 "/computronix_domi_permits_gcs.py --execution_date {{ ds }}",
    dag=dag
)

computronix_domi_permits_dataflow = BashOperator(
    task_id='computronix_domi_permits_dataflow',
    bash_command="python {}/dependencies/dataflow_scripts/computronix_domi_permits_dataflow.py --input gs://{}_"
                 "computronix/domi_permits/".format(os.environ['DAGS_PATH'], os.environ['GCS_PREFIX']) +
                 "{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ ds }}_domi_permits.json --avro_output " +
                 "gs://{}_computronix/domi_permits/avro_output/".format(os.environ['GCS_PREFIX']) +
                 "{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ ds }}/",
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
    bash_command=airflow_utils.beam_cleanup_statement('{}_computronix'.format(os.environ['GCS_PREFIX'])),
    dag=dag
)

computronix_domi_permits_gcs >> computronix_domi_permits_dataflow >> computronix_domi_permits_bq >> \
    (computronix_domi_permits_geojoin, computronix_domi_permits_beam_cleanup)
