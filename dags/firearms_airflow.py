from __future__ import absolute_import

import logging
import os

from airflow import DAG
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from datetime import timedelta
from dependencies import airflow_utils
from dependencies.airflow_utils import yesterday, build_revgeo_query

# TODO: When Airflow 2.0 is released, upgrade the package, upgrade the virtualenv to Python3,
# and add the arg py_interpreter='python3' to DataFlowPythonOperator

execution_date = "{{ execution_date }}"
prev_execution_date = "{{ prev_execution_date }}"

# We set the start_date of the DAG to the previous date, This will
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
    'firearm_seizures', default_args=default_args, schedule_interval='@monthly')

gcs_load = DockerOperator(
    task_id='firearms_gcs_docker',
    image='gcr.io/data-rivers/pgh-firearms',
    api_version='auto',
    auto_remove=True,
    environment={
        'APRS_UN': os.environ['APRS_UN'],
        'APRS_PW': os.environ['APRS_PW'],
        'GCS_AUTH_FILE': '/root/firearm-seizures-report/data-rivers-service-acct.json',
        'GCS_PREFIX': os.environ['GCS_PREFIX']
    },
    dag=dag
)

# dataflow_task = DataFlowPythonOperator(
#     task_id='firearms_dataflow',
#     job_name='firearms-dataflow',
#     py_file=os.getcwd() + '/airflow_scripts/dags/dependencies/dataflow_scripts/firearms_dataflow.py'),
#     dag=dag
# )

dataflow_task = BashOperator(
    task_id='firearms_dataflow',
    bash_command='python {}'.format(os.getcwd() + '/airflow_scripts/dags/dependencies/dataflow_scripts'
                                                  '/firearms_dataflow.py'),
    dag=dag
)

beam_cleanup = BashOperator(
    task_id='firearms_beam_cleanup',
    bash_command=airflow_utils.beam_cleanup_statement('{}_firearm_seizures'.format(os.environ['GCS_PREFIX'])),
    dag=dag
)

bq_insert_temp = GoogleCloudStorageToBigQueryOperator(
    task_id='firearms_bq_insert',
    destination_project_dataset_table='{}:firearm_seizures.seizures_temp'.format(os.environ['GCLOUD_PROJECT']),
    bucket='{}_firearm_seizures'.format(os.environ['GCS_PREFIX']),
    source_objects=["avro_output/{}/{}/{}/*.avro".format(execution_date.strftime('%Y'),
                                                         execution_date.strftime('%m').lower(),
                                                         execution_date.strftime("%Y-%m-%d"))],
    write_disposition='WRITE_APPEND',
    source_format='AVRO',
    time_partitioning={'type': 'DAY'},
    dag=dag
)

bq_geo_join = BigQueryOperator(
    task_id='firearms_bq_geojoin',
    sql=build_revgeo_query('firearm_seizures', 'seizures_temp'),
    use_legacy_sql=False,
    destination_dataset_table='{}:firearm_seizures.seizures'.format(os.environ['GCLOUD_PROJECT']),
    write_disposition='WRITE_APPEND',
    time_partitioning={'type': 'DAY'},
    dag=dag
)

bq_drop_temp = BigQueryOperator(
    task_id='firearms_bq_drop_temp',
    sql='DROP TABLE `{}.firearm_seizures.seizures_temp`'.format(os.environ['GCLOUD_PROJECT']),
    use_legacy_sql=False,
    dag=dag
)

gcs_load >> dataflow_task >> (bq_insert_temp, beam_cleanup) >> bq_geo_join >> bq_drop_temp
