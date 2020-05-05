from __future__ import absolute_import

import os

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
# from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from datetime import timedelta
from dependencies import airflow_utils
from dependencies.airflow_utils import yesterday, dt, build_revgeo_query, filter_old_values

# TODO: When Airflow 2.0 is released, upgrade the package, upgrade the virtualenv to Python3,
# and add the arg py_interpreter='python3' to DataFlowPythonOperator

# We set the start_date of the DAG to the previous date, as defined in airflow_utils. This will
# make the DAG immediately available for scheduling.

default_args = {
    'depends_on_past': False,
    'start_date': yesterday,
    'email': os.environ['EMAIL'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'project_id': os.environ['GCP_PROJECT'],
    'dataflow_default_options': {
        'project': os.environ['GCP_PROJECT']
    }
}

dag = DAG('comm_ctrs', default_args=default_args, schedule_interval=timedelta(days=1))

comm_ctrs_gcs = BashOperator(
    task_id='comm_ctrs_gcs',
    bash_command='python {}'.format(os.getcwd() + '/airflow_scripts/dags/dependencies/gcs_loaders'
                                                  '/community_centers_gcs.py'),
    dag=dag
)

comm_ctrs_dataflow = BashOperator(
    task_id='comm_ctrs_dataflow',
    bash_command='python {}'.format(os.getcwd() + '/airflow_scripts/dags/dependencies/dataflow_scripts'
                                                  '/comm_ctr_attendance_dataflow.py'),
    dag=dag
)

comm_ctrs_bq = GoogleCloudStorageToBigQueryOperator(
    task_id='comm_ctrs_bq',
    destination_project_dataset_table='{}:community_centers.attendance'.format(os.environ['GCP_PROJECT']),
    bucket='{}_community_centers'.format(os.environ['GCS_PREFIX']),
    source_objects=["attendance/avro_output/{}/{}/{}/*.avro".format(dt.strftime('%Y'),
                                                                    dt.strftime('%m').lower(),
                                                                    dt.strftime("%Y-%m-%d"))],
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    source_format='AVRO',
    time_partitioning={'type': 'DAY'},
    dag=dag
)

comm_ctrs_beam_cleanup = BashOperator(
    task_id='comm_ctrs_beam_cleanup',
    bash_command=airflow_utils.beam_cleanup_statement('{}_community_center'.format(os.environ['GCS_PREFIX'])),
    dag=dag
)

comm_ctrs_gcs >> comm_ctrs_dataflow >> (comm_ctrs_bq, comm_ctrs_beam_cleanup)
