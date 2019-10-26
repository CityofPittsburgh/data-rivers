import logging
import os

from airflow import DAG, configuration, models
from airflow.contrib.hooks import gcs_hook
from airflow.operators.docker_operator import DockerOperator
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta


# We set the start_date of the DAG to the previous date. This will
# make the DAG immediately available for scheduling.
YESTERDAY = datetime.combine(datetime.today() - timedelta(1), datetime.min.time())

default_args = {
    'depends_on_past': False,
    'start_date': YESTERDAY,
    'email': os.environ['EMAIL'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'project_id': os.environ['GCP_PROJECT'],
    'dataflow_default_options': {
        'project': os.environ['GCP_PROJECT'],
        'staging_location': 'gs://pghpa_trash_cans/staging',
        'temp_location': 'gs://pghpa_trash_cans/temp',
        'runner': 'DataflowRunner',
        'job_name': 'trash_cans'
    }
}

dag = DAG(
    'trash_cans', default_args=default_args, schedule_interval=timedelta(days=1))

gcs_load_task = DockerOperator(
    task_id='run_trash_can_docker_image',
    image='gcr.io/data-rivers/pgh-trash-can-api',
    api_version='auto',
    auto_remove=True,
    environment={
        'KEY': os.environ['AIRFLOW_TRASH_CAN_KEY'],
        'GCS_AUTH_FILE': '/root/trash-can-api/data-rivers-service-acct.json'
    },
    dag=dag
)

dataflow_task = DataFlowPythonOperator(
    task_id='trash_cans_dataflow',
    job_name='trash_cans_dataflow',
    py_file=os.environ['TRASH_CAN_DATAFLOW_FILE'],
    dag=dag
)

gcs_load_task >> dataflow_task