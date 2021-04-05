from __future__ import absolute_import

import os

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_year, get_ds_month, default_args


dag = DAG(
    'police_demographics',
    default_args=default_args,
    schedule_interval='@monthly',
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year}
)

police_demographics_gcs = BashOperator(
    task_id='police_demographics_gcs',
    bash_command='python {}'.format(os.environ['DAGS_PATH'] + '/dependencies/gcs_loaders/police_demographics_gcs.py --execution_date {{ ds }} --prev_execution_date {{ prev_ds }}'),
    dag=dag
)

police_demographics_dataflow = BashOperator(
    task_id='police_demographics_dataflow',
    bash_command="python {}/dependencies/dataflow_scripts/police_demographics_dataflow.py --input gs://{}_police/"
                 "officer_demographics/".format(os.environ['DAGS_PATH'], os.environ['GCS_PREFIX']) + "{{ ds|get_ds_year }}/"
                 "{{ ds|get_ds_month }}/{{ ds }}_demographics.json --avro_output " + "gs://{}_police/"
                 "officer_demographics/avro_output/".format(os.environ['GCS_PREFIX']) + "{{ ds|get_ds_year }}/"
                 "{{ ds|get_ds_month }}/{{ ds }}/",
    dag=dag
)

# this table gets overrwritten since it will contain newly hired officers and ALL previously employed officers. It would be possible to first search officers that are already in the database, and only add officers that are not present.  However, this is unnecessarily complicated at the present (April 2021). 

police_demographics_bq = GoogleCloudStorageToBigQueryOperator(
    task_id='police_demographics_bq_load',
    destination_project_dataset_table='{}:public_safety.police_demographics'.format(os.environ['GCLOUD_PROJECT']),
    bucket='{}_police'.format(os.environ['GCS_PREFIX']),
    source_objects=["officer_demographics/avro_output/{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ ds }}/*.avro"],
    write_disposition='WRITE_TRUNCATE',
    source_format='AVRO',
    dag=dag
)

police_demographics_beam_cleanup = BashOperator(
    task_id='police_demographics_beam_cleanup',
    bash_command=airflow_utils.beam_cleanup_statement('{}_police'.format(os.environ['GCS_PREFIX'])),
    dag=dag
)

police_demographics_gcs >> police_demographics_dataflow>>police_demographics_bq >> police_demographics_beam_cleanup
