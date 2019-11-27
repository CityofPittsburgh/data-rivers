from __future__ import absolute_import

import logging
import os

from datetime import datetime

from airflow import DAG, configuration, models
from airflow.contrib.hooks import gcs_hook
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator
from airflow.contrib.operators import gcs_to_bq
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
from dependencies import airflow_utils


#TODO: When Airflow 2.0 is released, upgrade the package, upgrade the virtualenv to Python3,
# and add the arg py_interpreter='python3' to DataFlowPythonOperator

# We set the start_date of the DAG to the previous date. This will
# make the DAG immediately available for scheduling.

YESTERDAY = datetime.combine(datetime.today() - timedelta(1), datetime.min.time())
dt = datetime.now()

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
        'project': os.environ['GCP_PROJECT']
    }
}

dag = DAG(
    'trash_cans', default_args=default_args, schedule_interval=timedelta(days=1))

gcs_load = DockerOperator(
    task_id='trash_cans_gcs_docker',
    image='gcr.io/data-rivers/pgh-trash-can-api',
    api_version='auto',
    auto_remove=True,
    environment={
        'KEY': os.environ['AIRFLOW_TRASH_CAN_KEY'],
        'GCS_AUTH_FILE': '/root/trash-can-api/data-rivers-service-acct.json'
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
    bash_command='python {}'.format(os.environ['TRASH_CAN_DATAFLOW_FILE']),
    dag=dag
)

bq_insert = BashOperator(
    task_id='trash_cans_bq_insert',
    bash_command='bq load --source_format=AVRO trash_cans.smart_trash_cans \
    "gs://pghpa_trash_cans/avro_output/{}/{}/{}/*.avro"'.format(dt.strftime('%Y'),
                                                                dt.strftime('%m').lower(),
                                                                dt.strftime("%Y-%m-%d")),
    dag=dag

)

# bq_insert = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
#     task_id='trash_cans_bq_insert',
#     bucket='pghpa_trash_cans',
#     source_objects='avro_output/*.avro',
#     destination_project_dataset_table='trash_cans.smart_trash_cans',
#     write_disposition='WRITE_APPEND',
#     dag=dag
# )

beam_cleanup = BashOperator(
    task_id='trash_cans_beam_cleanup',
    bash_command='gsutil rm gs://pghpa_trash_cans/beam_output/**',
    dag=dag
)

gcs_load >> dataflow_task >> (bq_insert,  beam_cleanup)
