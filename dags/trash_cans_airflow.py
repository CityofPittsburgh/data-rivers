from __future__ import absolute_import

import os

from airflow import DAG
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from dependencies import airflow_utils
from dependencies.airflow_utils import yesterday, get_ds_year, get_ds_month, default_args


#TODO: When Airflow 2.0 is released, upgrade the package, upgrade the virtualenv to Python3,
# and add the arg py_interpreter='python3' to DataFlowPythonOperator

dag = DAG(
    'trash_cans',
    default_args=default_args,
    schedule_interval='@weekly',
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year}
)

gcs_load = DockerOperator(
    task_id='trash_cans_gcs_docker',
    image='gcr.io/data-rivers/pgh-trash-can-api',
    api_version='auto',
    auto_remove=True,
    environment={
        'KEY': os.environ['AIRFLOW_TRASH_CAN_KEY'],
        'GCS_AUTH_FILE': '/root/trash-can-api/data-rivers-service-acct.json',
        'GCS_PREFIX': os.environ['GCS_PREFIX']
    },
    dag=dag
)

dataflow_task = BashOperator(
    task_id='trash_cans_dataflow',
    bash_command="python {}dependencies/dataflow_scripts/trash_cans_dataflow.py --input gs://{}_trash_cans/"
                 .format(os.environ['DAGS_PATH'], os.environ['GCS_PREFIX']) + "{{ ds|get_ds_year }}/"
                 "{{ ds|get_ds_month }}/{{ ds }}_smart_trash_containers.csv --avro_output " + "gs://{}_trash_cans/"
                 "avro_output/".format(os.environ['GCS_PREFIX']) + "{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ ds }}/",
    dag=dag
)


bq_insert = GoogleCloudStorageToBigQueryOperator(
    task_id='trash_cans_bq_insert',
    destination_project_dataset_table='{}:trash_cans.containers'.format(os.environ['GCLOUD_PROJECT']),
    bucket='{}_trash_cans'.format(os.environ['GCS_PREFIX']),
    source_objects=["avro_output/{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ ds }}/*.avro"],
    write_disposition='WRITE_APPEND',
    source_format='AVRO',
    time_partitioning={'type': 'DAY'},
    dag=dag
)

beam_cleanup = BashOperator(
    task_id='trash_cans_beam_cleanup',
    bash_command=airflow_utils.beam_cleanup_statement('{}_trash_cans'.format(os.environ['GCS_PREFIX'])),
    dag=dag
)

gcs_load >> dataflow_task >> (bq_insert,  beam_cleanup)
