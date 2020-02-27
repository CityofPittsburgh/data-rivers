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
from dependencies.airflow_utils import YESTERDAY, dt

#TODO: When Airflow 2.0 is released, upgrade the package, upgrade the virtualenv to Python3,
# and add the arg py_interpreter='python3' to DataFlowPythonOperator

# We set the start_date of the DAG to the previous date, as defined in airflow_utils. This will
# make the DAG immediately available for scheduling.

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
    'qalert', default_args=default_args, schedule_interval=timedelta(days=1))


qalert_gcs = BashOperator(
    task_id='qalert_gcs',
    bash_command='python {}'.format(os.getcwd() + '/airflow_scripts/dags/dependencies/gcs_loaders'
                                                  '/311_gcs.py'),
    dag=dag
)


qalert_requests_dataflow = BashOperator(
    task_id='qalert_requests_dataflow',
    bash_command='python {}'.format(os.getcwd() + '/airflow_scripts/dags/dependencies/dataflow_scripts'
                                                  '/qalert_requests_dataflow.py'),
    dag=dag
)

qalert_activities_dataflow = BashOperator(
    task_id='qalert_activities_dataflow',
    bash_command='python {}'.format(os.getcwd() + '/airflow_scripts/dags/dependencies/dataflow_scripts'
                                                  '/qalert_activities_dataflow.py'),
    dag=dag
)


qalert_requests_bq = GoogleCloudStorageToBigQueryOperator(
    task_id='qalert_requests_bq',
    destination_project_dataset_table='{}:311.requests'.format(os.environ['GCP_PROJECT']),
    bucket='{}_311'.format(os.environ['GCS_PREFIX']),
    source_objects=["requests/avro_output/{}/{}/{}/*.avro".format(dt.strftime('%Y'),
                                                         dt.strftime('%m').lower(),
                                                         dt.strftime("%Y-%m-%d"))],
    write_disposition='WRITE_EMPTY',
    create_disposition='CREATE_IF_NEEDED',
    source_format='AVRO',
    time_partitioning={'type': 'DAY'},
    dag=dag
)

qalert_activities_bq = GoogleCloudStorageToBigQueryOperator(
    task_id='qalert_activities_bq',
    destination_project_dataset_table='{}:311.activities'.format(os.environ['GCP_PROJECT']),
    bucket='{}_311'.format(os.environ['GCS_PREFIX']),
    source_objects=["activities/avro_output/{}/{}/{}/*.avro".format(dt.strftime('%Y'),
                                                         dt.strftime('%m').lower(),
                                                         dt.strftime("%Y-%m-%d"))],
    write_disposition='WRITE_EMPTY',
    create_disposition='CREATE_IF_NEEDED',
    source_format='AVRO',
    time_partitioning={'type': 'DAY'},
    dag=dag
)

beam_cleanup = BashOperator(
    task_id='qalert_beam_cleanup',
    bash_command=airflow_utils.beam_cleanup_statement('{}_311'.format(os.environ['GCS_PREFIX'])),
    dag=dag
)


qalert_gcs >> qalert_requests_dataflow >> (qalert_requests_bq, beam_cleanup)
qalert_gcs >> qalert_activities_dataflow >> (qalert_activities_bq, beam_cleanup)