from __future__ import absolute_import

import os

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_year, get_ds_month, default_args


dag = DAG(
    'police_blotter_30_day',
    default_args=default_args,
    schedule_interval='@daily',
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year}
)

police_blotter_30_day_gcs = BashOperator(
    task_id='police_blotter_30_day_gcs',
    bash_command='python {}'.format(os.environ['DAGS_PATH'] + '/dependencies/gcs_loaders/police_blotter_30_day_gcs.py '
                                                              '--execution_date {{ ds }}'),
    dag=dag
)

police_blotter_30_day_dataflow = BashOperator(
    task_id='police_blotter_30_day_dataflow',
    bash_command="python {}/dependencies/dataflow_scripts/police_blotter_30_day_dataflow.py --input gs://{}_police/"
                 "30_day_blotter/".format(os.environ['DAGS_PATH'], os.environ['GCS_PREFIX']) + "{{ ds|get_ds_year }}/"
                 "{{ ds|get_ds_month }}/{{ ds }}_blotter.json --avro_output " + "gs://{}_police/"
                 "30_day_blotter/avro_output/".format(os.environ['GCS_PREFIX']) + "{{ ds|get_ds_year }}/"
                 "{{ ds|get_ds_month }}/{{ ds }}/",
    dag=dag
)

# this table gets overrwritten with each job since it's only meant to cover the past 30 days

police_blotter_30_day_bq = GoogleCloudStorageToBigQueryOperator(
    task_id='police_blotter_30_day_bq_load',
    destination_project_dataset_table='{}:public_safety.30_day_blotter_raw'.format(os.environ['GCLOUD_PROJECT']),
    bucket='{}_police'.format(os.environ['GCS_PREFIX']),
    source_objects=["30_day_blotter/avro_output/{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ ds }}/*.avro"],
    write_disposition='WRITE_TRUNCATE',
    source_format='AVRO',
    time_partitioning={'type': 'DAY'},
    dag=dag
)

#TODO: BigQueryOperator to rev-geocode based on tract

police_blotter_30_day_beam_cleanup = BashOperator(
    task_id='police_blotter_30_day_beam_cleanup',
    bash_command=airflow_utils.beam_cleanup_statement('{}_police'.format(os.environ['GCS_PREFIX'])),
    dag=dag
)

police_blotter_30_day_gcs >> police_blotter_30_day_dataflow >> police_blotter_30_day_bq >> \
    police_blotter_30_day_beam_cleanup
