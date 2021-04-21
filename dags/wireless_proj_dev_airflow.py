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
    'wireless_proj_dev',
    default_args=default_args,
    schedule_interval='@monthly',
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year}
)

wireless_proj_dev_gcs = BashOperator(
    task_id='wireless_proj_dev_gcs',
    bash_command='python {}'.format(os.environ['DAGS_PATH'] + '/dependencies/gcs_loaders/wireless_proj_dev_gcs.py '
                                                              '--execution_date {{ ds }}'),
    dag=dag
)

wireless_proj_dev_dataflow = BashOperator(
    task_id='wireless_proj_dev_dataflow',
    bash_command="python {}/dependencies/dataflow_scripts/wireless_proj_dev_dataflow.py --input gs://{}_wireless_proj_dev/"
                 "wireless_usage_cost/".format(os.environ['DAGS_PATH'], os.environ['GCS_PREFIX']) + "{{ ds|get_ds_year }}/"
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