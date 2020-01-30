from __future__ import absolute_import

import logging
import os

from datetime import datetime, timedelta
from airflow import DAG, configuration, models
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator
from airflow.contrib.operators.gcp_container_operator import GKEPodOperator
from airflow.operators.python_operator import PythonOperator
from dependencies import airflow_utils
from dependencies.airflow_utils import YESTERDAY, dt, bq_client, storage_client


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

computronix_gcs = DockerOperator(
    task_id='computronix_gcs',
    image='gcr.io/data-rivers/pgh-computronix',
    api_version='auto',
    auto_remove=True,
    environment={
        'GCS_AUTH_FILE': '/root/odata-computronix/data-rivers-service-acct.json'
    },
    dag=dag
)

# gcs_load = GKEPodOperator(
#     task_id='computronix_gcs_load',
#     cluster_name='us-east1-data-rivers-205ba4d7-gk',
#     image='gcr.io/data-rivers/pgh-computronix',
#     env_vars={
#             'GCS_AUTH_FILE': '/root/odata-computronix/data-rivers-service-acct.json'
#         },
#     dag=dag
# )

computronix_trades_dataflow = BashOperator(
    task_id='computronix_trades_dataflow',
    bash_command='python {}'.format(os.getcwd() + '/airflow_scripts/dags/dependencies/dataflow_scripts'
                                                  '/computronix_trades_dataflow.py'),
    dag=dag
)

computronix_contractors_dataflow = BashOperator(
    task_id='computronix_contractors_dataflow',
    bash_command='python {}'.format(os.getcwd() + '/airflow_scripts/dags/dependencies/dataflow_scripts'
                                                  '/computronix_contractors_dataflow.py'),
    dag=dag
)

computronix_businesses_dataflow = BashOperator(
    task_id='computronix_businesses_dataflow',
    bash_command='python {}'.format(os.getcwd() + '/airflow_scripts/dags/dependencies/dataflow_scripts'
                                                  '/computronix_businesses_dataflow.py'),
    dag=dag
)


computronix_trades_bq = GoogleCloudStorageToBigQueryOperator(
    task_id='computronix_trades_bq',
    destination_project_dataset_table='{}:computronix.trades'.format(os.environ['GCP_PROJECT']),
    bucket='{}_computronix'.format(os.environ['GCS_PREFIX']),
    source_objects=["trades/avro_output/{}/{}/{}/*.avro".format(dt.strftime('%Y'),
                                                         dt.strftime('%m').lower(),
                                                         dt.strftime("%Y-%m-%d"))],
    write_disposition='WRITE_APPEND',
    source_format='AVRO',
    time_partitioning={'type': 'DAY'},
    dag=dag
)


computronix_contractors_bq = GoogleCloudStorageToBigQueryOperator(
    task_id='computronix_contractors_bq',
    destination_project_dataset_table='{}:computronix.contractors'.format(os.environ['GCP_PROJECT']),
    bucket='{}_computronix'.format(os.environ['GCS_PREFIX']),
    source_objects=["contractors/avro_output/{}/{}/{}/*.avro".format(dt.strftime('%Y'),
                                                         dt.strftime('%m').lower(),
                                                         dt.strftime("%Y-%m-%d"))],
    write_disposition='WRITE_APPEND',
    source_format='AVRO',
    time_partitioning={'type': 'DAY'},
    dag=dag
)


computronix_businesses_bq = GoogleCloudStorageToBigQueryOperator(
    task_id='computronix_businesses_bq',
    destination_project_dataset_table='{}:computronix.businesses'.format(os.environ['GCP_PROJECT']),
    bucket='{}_computronix'.format(os.environ['GCS_PREFIX']),
    source_objects=["businesses/avro_output/{}/{}/{}/*.avro".format(dt.strftime('%Y'),
                                                         dt.strftime('%m').lower(),
                                                         dt.strftime("%Y-%m-%d"))],
    write_disposition='WRITE_APPEND',
    source_format='AVRO',
    time_partitioning={'type': 'DAY'},
    dag=dag
)


beam_cleanup = BashOperator(
    task_id='computronix_beam_cleanup',
    bash_command=airflow_utils.beam_cleanup_statement('{}_computronix'.format(os.environ['GCS_PREFIX'])),
    dag=dag
)


computronix_gcs >> computronix_trades_dataflow >> (computronix_trades_bq, beam_cleanup)
computronix_gcs >> computronix_contractors_dataflow >> (computronix_contractors_bq, beam_cleanup)
computronix_gcs >> computronix_businesses_dataflow >> (computronix_businesses_bq, beam_cleanup)
