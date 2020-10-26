from __future__ import absolute_import

import os

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_year, get_ds_month, default_args

# TODO: When Airflow 2.0 is released, upgrade the package, sub in DataFlowPythonOperator for BashOperator,
# and pass the argument 'py_interpreter=python3'

dag = DAG(
    'twilio',
    default_args=default_args,
    schedule_interval='@daily',
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year}
)

twilio_gcs = BashOperator(
    task_id='twilio_gcs',
    bash_command='python {}'.format(os.environ['DAGS_PATH'] + '/dependencies/gcs_loaders/twilio_gcs.py '
                                                              '--execution_date {{ ds }}'),
    dag=dag
)

twilio_311_dataflow = BashOperator(
    task_id='twilio_311_dataflow',
    bash_command="python {}/dependencies/dataflow_scripts/twilio_311_dataflow.py --input gs://{}_twilio/311/"
                 .format(os.environ['DAGS_PATH'], os.environ['GCS_PREFIX']) + "{{ ds|get_ds_year }}/"
                 "{{ ds|get_ds_month }}/{{ ds }}_311.json --avro_output " + "gs://{}_twilio/311/avro_output/"
                 .format(os.environ['GCS_PREFIX']) + "{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ ds }}/",
    dag=dag
)

twilio_service_desk_dataflow = BashOperator(
    task_id='twilio_service_desk_dataflow',
    bash_command="python {}/dependencies/dataflow_scripts/twilio_service_desk_dataflow.py --input gs://{}_twilio/"
                 "service_desk/".format(os.environ['DAGS_PATH'], os.environ['GCS_PREFIX']) + "{{ ds|get_ds_year }}/"
                 "{{ ds|get_ds_month }}/{{ ds }}_service_desk.json --avro_output " + "gs://{}_twilio/service_desk/"
                 "avro_output/".format(os.environ['GCS_PREFIX']) + "{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ ds "
                                                                   "}}/",
    dag=dag
)

twilio_311_bq = GoogleCloudStorageToBigQueryOperator(
    task_id='twilio_311_bq',
    destination_project_dataset_table='{}:twilio.311'.format(os.environ['GCLOUD_PROJECT']),
    bucket='{}_twilio'.format(os.environ['GCS_PREFIX']),
    source_objects=["311/avro_output/{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ ds }}/*.avro"],

    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    source_format='AVRO',
    autodetect=True,
    dag=dag
)

# ^maybe a bug in this operator; you should not have to specify autodetect with an avro source

twilio_service_desk_bq = GoogleCloudStorageToBigQueryOperator(
    task_id='twilio_service_desk_bq',
    destination_project_dataset_table='{}:twilio.service_desk'.format(os.environ['GCLOUD_PROJECT']),
    bucket='{}_twilio'.format(os.environ['GCS_PREFIX']),
    source_objects=["service_desk/avro_output/{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ ds }}/*.avro"],
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    source_format='AVRO',
    autodetect=True,
    dag=dag
)

twilio_beam_cleanup = BashOperator(
    task_id='twilio_beam_cleanup',
    bash_command=airflow_utils.beam_cleanup_statement('{}_twilio'.format(os.environ['GCS_PREFIX'])),
    dag=dag
)

twilio_gcs >> twilio_311_dataflow >> (twilio_311_bq, twilio_beam_cleanup)

twilio_gcs >> twilio_service_desk_dataflow >> (twilio_service_desk_bq, twilio_beam_cleanup)
