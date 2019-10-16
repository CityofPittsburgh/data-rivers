import os

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.docker_operator import DockerOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2015, 6, 1),
    'email': ['james.otoole@pittsburghpa.gov'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'trash_cans', default_args=default_args, schedule_interval=timedelta(days=1))

t1 = DockerOperator(
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

t1