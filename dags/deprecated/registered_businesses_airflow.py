from __future__ import absolute_import

import os

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

from deprecated import airflow_utils
from deprecated.airflow_utils import get_ds_year, get_ds_month, default_args

dag = DAG(
    'registered_businesses',
    default_args=default_args,
    schedule_interval='@monthly',
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year}
)

registered_businesses_gcs = BashOperator(
    task_id='registered_businesses_gcs',
    bash_command='python {}'.format(os.environ['DAGS_PATH'] + '/dependencies/gcs_loaders/registered_businesses_gcs.py '
                                                              '--execution_date {{ ds }}'),
    dag=dag
)
registered_businesses_dataflow = BashOperator(
    task_id='registered_businesses_dataflow',
    bash_command="python {}/dependencies/dataflow_scripts/registered_businesses_dataflow.py --input gs://{}_finance/"
                 "businesses/".format(os.environ['DAGS_PATH'], os.environ['GCS_PREFIX']) + "{{ ds|get_ds_year }}/"
                 "{{ ds|get_ds_month }}/{{ ds }}_businesses.json --avro_output " + "gs://{}_finance/"
                 "businesses/avro_output/".format(os.environ['GCS_PREFIX']) + "{{ ds|get_ds_year }}/{{ ds|get_ds_month "
                 "}}/{{ ds }}/",
    dag=dag
)

# there are only about 20k businesses and the job is monthly, so fine to just overwrite the table every time

registered_businesses_bq_load = GoogleCloudStorageToBigQueryOperator(
    task_id='registered_businesses_bq_load',
    destination_project_dataset_table='{}:finance.registered_businesses'.format(os.environ['GCLOUD_PROJECT']),
    bucket='{}_finance'.format(os.environ['GCS_PREFIX']),
    source_objects=["businesses/avro_output/{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ ds }}/*.avro"],
    write_disposition='WRITE_TRUNCATE',
    source_format='AVRO',
    time_partitioning={'type': 'DAY'},
    dag=dag
)


registered_businesses_beam_cleanup = BashOperator(
    task_id='registered_businesses_beam_cleanup',
    bash_command= airflow_utils.beam_cleanup_statement('{}_finance'.format(os.environ['GCS_PREFIX'])),
    dag=dag
)

registered_businesses_gcs >> registered_businesses_dataflow >> (registered_businesses_bq_load,
                                                                registered_businesses_beam_cleanup)
