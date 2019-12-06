from __future__ import absolute_import

import logging
import os

from datetime import datetime, timedelta
from airflow import DAG, configuration, models
from airflow.contrib.hooks import gcs_hook
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator
from airflow.contrib.operators import gcs_to_bq
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from dependencies import airflow_utils
from dependencies.airflow_utils import YESTERDAY, dt


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
    'computronix', default_args=default_args, schedule_interval=timedelta(days=1))

gcs_load = DockerOperator(
    task_id='computronix_gcs_docker',
    image='gcr.io/data-rivers/pgh-computronix',
    api_version='auto',
    auto_remove=True,
    environment={
        'GCS_AUTH_FILE': '/root/odata-computronix/data-rivers-service-acct.json'
    },
    dag=dag
)

# dataflow_task = DataFlowPythonOperator(
#     task_id='trash_cans_dataflow',
#     job_name='trash-cans-dataflow',
#     py_file=os.environ['TRASH_CAN_DATAFLOW_FILE'],
#     dag=dag
# )

# dataflow_task = BashOperator(
#     task_id='computronix_dataflow',
#     bash_command='python {}'.format(os.environ['TRASH_CAN_DATAFLOW_FILE']),
#     dag=dag
# )
#
# bq_insert = BashOperator(
#     task_id='trash_cans_bq_insert',
#     bash_command='bq load --source_format=AVRO trash_cans.smart_trash_cans \
#     "gs://pghpa_trash_cans/avro_output/{}/{}/{}/*.avro"'.format(dt.strftime('%Y'),
#                                                                 dt.strftime('%m').lower(),
#                                                                 dt.strftime("%Y-%m-%d")),
#     dag=dag
#
# )
#
# beam_cleanup = BashOperator(
#     task_id='computronix_beam_cleanup',
#     bash_command=airflow_utils.beam_cleanup_statement('pghpa_computronix'),
#     dag=dag
# )

gcs_load

# gcs_load >> dataflow_task >> (bq_insert,  beam_cleanup)
