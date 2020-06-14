from __future__ import absolute_import

import logging
import os

from datetime import datetime

from airflow import DAG, configuration, models
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
from dependencies import airflow_utils
from dependencies.airflow_utils import yesterday


#TODO: When Airflow 2.0 is released, upgrade the package, upgrade the virtualenv to Python3,
# and add the arg py_interpreter='python3' to DataFlowPythonOperator

execution_date = "{{ execution_date }}"
prev_execution_date = "{{ prev_execution_date }}"

# We set the start_date of the DAG to the previous date, as defined in airflow_utils. This will
# make the DAG immediately available for scheduling.

default_args = {
    'depends_on_past': False,
    'start_date': yesterday,
    'email': os.environ['EMAIL'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'project_id': os.environ['GCLOUD_PROJECT'],
    'dataflow_default_options': {
        'project': os.environ['GCLOUD_PROJECT']
    }
}

dag = DAG(
    'trash_cans', default_args=default_args, schedule_interval='@daily')

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

# dataflow_task = DataFlowPythonOperator(
#     task_id='trash_cans_dataflow',
#     job_name='trash-cans-dataflow',
#     py_file=os.environ['TRASH_CAN_DATAFLOW_FILE'],
#     dag=dag
# )

dataflow_task = BashOperator(
    task_id='trash_cans_dataflow',
    bash_command='python {}'.format(os.getcwd() + '/airflow_scripts/dags/dependencies/dataflow_scripts'
                                                  '/trash_cans_dataflow.py'),
    dag=dag
)


bq_insert = GoogleCloudStorageToBigQueryOperator(
    task_id='trash_cans_bq_insert',
    destination_project_dataset_table='{}:trash_cans.containers'.format(os.environ['GCLOUD_PROJECT']),
    bucket='{}_trash_cans'.format(os.environ['GCS_PREFIX']),
    source_objects=["avro_output/{}/{}/{}/*.avro".format(execution_date.strftime('%Y'),
                                                         execution_date.strftime('%m').lower(),
                                                         execution_date.strftime("%Y-%m-%d"))],
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
