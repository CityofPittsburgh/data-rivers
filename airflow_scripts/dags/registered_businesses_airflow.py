from __future__ import absolute_import

import logging
import os

from airflow import DAG, configuration, models
from airflow.contrib.hooks import gcs_hook
from airflow.operators.docker_operator import DockerOperator
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
from dependencies import airflow_utils
from dependencies.airflow_utils import yesterday, dt


default_args = {
    'depends_on_past': False,
    'start_date': yesterday,
    'email': os.environ['EMAIL'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'project_id': os.environ['GCP_PROJECT'],
    'dataflow_default_options': {
        'project': os.environ['GCP_PROJECT'],
        'staging_location': 'gs://pghpa_finance/staging',
        'temp_location': 'gs://pghpa_finance/temp',
        'runner': 'DataflowRunner',
        'job_name': 'finance_open_data'
    }
}

dag = DAG(
    'registered_businesses', default_args=default_args, schedule_interval='@monthly')

gcs_load_task = DockerOperator(
    task_id='registered_businesses_gcs',
    image='gcr.io/data-rivers/pgh-finance',
    api_version='auto',
    auto_remove=True,
    environment={
        'ISAT_UN': os.environ['ISAT_UN'],
        'ISAT_PW': os.environ['ISAT_PW'],
        'GCS_AUTH_FILE': '/root/finance-open-data/data-rivers-service-acct.json'
    },
    dag=dag
)


dataflow_task = DataFlowPythonOperator(
    task_id='registered_businesses_dataflow',
    job_name='registered-businesses-dataflow_scripts',
    py_file=(os.getcwd() + '/airflow_scripts/dags/dependencies/dataflow_scripts/registered_businesses_dataflow.py'),
    dag=dag
)


# bq_insert = GoogleCloudStorageToBigQueryOperator(
#     task_id='registered_businesses_bq_insert',
#     destination_project_dataset_table='{}:registered_businesses.registered_businesses'.format(os.environ['GCP_PROJECT']),
#     bucket='{}_finance'.format(os.environ['GCS_PREFIX']),
#     source_objects=["avro_output/{}/{}/{}/*.avro".format(dt.strftime('%Y'),
#                                                          dt.strftime('%m').lower(),
#                                                          dt.strftime("%Y-%m-%d"))],
#     write_disposition='WRITE_APPEND',
#     source_format='AVRO',
#     time_partitioning={'type': 'DAY'},
#     dag=dag
# )


gcs_load_task >> dataflow_task
