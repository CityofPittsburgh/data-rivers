from __future__ import absolute_import

import os

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
# from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from dependencies import airflow_utils
from dependencies.airflow_utils import build_revgeo_query, filter_old_values, get_ds_month, get_ds_year, default_args

# TODO: When Airflow 2.0 is released, upgrade the package, upgrade the virtualenv to Python3,
# and add the arg py_interpreter='python3' to DataFlowPythonOperator

# We set the start_date of the DAG to the previous date, as defined in airflow_utils. This will
# make the DAG immediately available for scheduling.

dag = DAG(
    'comm_ctrs',
    default_args=default_args,
    schedule_interval='@weekly',
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year}
)

comm_ctrs_gcs = BashOperator(
    task_id='comm_ctrs_gcs',
    bash_command='python {}'.format(os.environ['DAGS_PATH'] + '/dependencies/gcs_loaders/community_centers_gcs.py '
                                                              '--execution_date {{ ds }}'),
    dag=dag
)

comm_ctrs_dataflow = BashOperator(
    task_id='comm_ctrs_dataflow',
    bash_command="python {}/dependencies/dataflow_scripts/comm_ctr_attendance_dataflow.py --input "
                 "gs://{}_community_centers/attendance/".format(os.environ['DAGS_PATH'], os.environ['GCS_PREFIX']) +
                 "{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ ds }}_attendance.json --avro_output " +
                 "gs://{}_community_centers/attendance/avro_output/".format(os.environ['GCS_PREFIX']) +
                 "{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ ds }}/",
    dag=dag
)

comm_ctrs_bq = GoogleCloudStorageToBigQueryOperator(
    task_id='comm_ctrs_bq',
    destination_project_dataset_table='{}:community_centers.attendance'.format(os.environ['GCLOUD_PROJECT']),
    bucket='{}_community_centers'.format(os.environ['GCS_PREFIX']),
    source_objects=["attendance/avro_output/{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ ds }}/*.avro"],
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    source_format='AVRO',
    autodetect=True,
    time_partitioning={'type': 'DAY'},
    dag=dag
)

comm_ctrs_beam_cleanup = BashOperator(
    task_id='comm_ctrs_beam_cleanup',
    bash_command=airflow_utils.beam_cleanup_statement('{}_community_center'.format(os.environ['GCS_PREFIX'])),
    dag=dag
)

comm_ctrs_gcs >> comm_ctrs_dataflow >> (comm_ctrs_bq, comm_ctrs_beam_cleanup)
