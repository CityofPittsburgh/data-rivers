from __future__ import absolute_import

import os

from airflow import DAG
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from deprecated import airflow_utils
from deprecated.airflow_utils import get_ds_month, get_ds_year, default_args


dag = DAG(
    'computronix_licenses',
    default_args=default_args,
    schedule_interval='@monthly',
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year}
)

computronix_gcs = DockerOperator(
    task_id='computronix_gcs',
    image='gcr.io/data-rivers/pgh-computronix',
    api_version='auto',
    environment={
        'GCS_AUTH_FILE': '/root/odata-computronix/data-rivers-service-acct.json',
        'GCS_PREFIX': os.environ['GCS_PREFIX'],
        'execution_date': '{{ ds }}',
        'execution_month': '{{ ds|get_ds_year }}' + '/' + '{{ ds|get_ds_month }}'
    },
    dag=dag
)

#TODO: Rewrite the script executed by this DockerOperator
# (https://github.com/CityofPittsburgh/OData-Computronix/blob/master/contractors_gcp.R) in Python and execute via
# BashOperator

computronix_trades_dataflow = BashOperator(
    task_id='computronix_trades_dataflow',
    bash_command="python {}/dependencies/dataflow_scripts/computronix_trades_dataflow.py --input gs://{}_computronix/"
                 "trades/".format(os.environ['DAGS_PATH'], os.environ['GCS_PREFIX']) + "{{ ds|get_ds_year }}/{{ "
                 "ds|get_ds_month }}/{{ ds }}_trades_licenses.json --avro_output " + "gs://{}_computronix/trades/"
                 "avro_output/".format(os.environ['GCS_PREFIX']) + "{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/"
                 "{{ ds }}/",
    dag=dag
)

computronix_contractors_dataflow = BashOperator(
    task_id='computronix_contractors_dataflow',
    bash_command="python {}/dependencies/dataflow_scripts/computronix_contractors_dataflow.py --input gs://{}_"
                 "computronix/contractors/".format(os.environ['DAGS_PATH'], os.environ['GCS_PREFIX']) + "{{ "
                 "ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ ds }}_contractors_licenses.json --avro_output " +
                 "gs://{}_computronix/contractors/avro_output/".format(os.environ['GCS_PREFIX']) +
                 "{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ ds }}/",
    dag=dag
)

computronix_businesses_dataflow = BashOperator(
    task_id='computronix_businesses_dataflow',
    bash_command="python {}/dependencies/dataflow_scripts/computronix_businesses_dataflow.py --input gs://{}_"
                 "computronix/businesses/".format(os.environ['DAGS_PATH'], os.environ['GCS_PREFIX']) + "{{ "
                 "ds|get_ds_year }}/{{ ds|get_ds_month}}/{{ ds }}_business_licenses.json --avro_output " +
                 "gs://{}_computronix/businesses/avro_output/".format(os.environ['GCS_PREFIX']) + "{{ ds|get_ds_year }}"
                 "/{{ ds|get_ds_month }}/{{ ds }}/",
    dag=dag
)


computronix_trades_bq = GoogleCloudStorageToBigQueryOperator(
    task_id='computronix_trades_bq',
    destination_project_dataset_table='{}:computronix.trades'.format(os.environ['GCLOUD_PROJECT']),
    bucket='{}_computronix'.format(os.environ['GCS_PREFIX']),
    source_objects=["trades/avro_output/{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ ds }}/*.avro"],
    write_disposition='WRITE_TRUNCATE',
    source_format='AVRO',
    time_partitioning={'type': 'DAY'},
    dag=dag
)


computronix_contractors_bq = GoogleCloudStorageToBigQueryOperator(
    task_id='computronix_contractors_bq',
    destination_project_dataset_table='{}:computronix.contractors'.format(os.environ['GCLOUD_PROJECT']),
    bucket='{}_computronix'.format(os.environ['GCS_PREFIX']),
    source_objects=["contractors/avro_output/{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ ds }}/*.avro"],
    write_disposition='WRITE_TRUNCATE',
    source_format='AVRO',
    time_partitioning={'type': 'DAY'},
    dag=dag
)


computronix_businesses_bq = GoogleCloudStorageToBigQueryOperator(
    task_id='computronix_businesses_bq',
    destination_project_dataset_table='{}:computronix.businesses'.format(os.environ['GCLOUD_PROJECT']),
    bucket='{}_computronix'.format(os.environ['GCS_PREFIX']),
    source_objects=["businesses/avro_output/{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ ds }}/*.avro"],
    write_disposition='WRITE_TRUNCATE',
    source_format='AVRO',
    time_partitioning={'type': 'DAY'},
    dag=dag
)


beam_cleanup = BashOperator(
    task_id='computronix_beam_cleanup',
    bash_command= airflow_utils.beam_cleanup_statement('{}_computronix'.format(os.environ['GCS_PREFIX'])),
    dag=dag
)


computronix_gcs >> computronix_trades_dataflow >> (computronix_trades_bq, beam_cleanup)
computronix_gcs >> computronix_contractors_dataflow >> (computronix_contractors_bq, beam_cleanup)
computronix_gcs >> computronix_businesses_dataflow >> (computronix_businesses_bq, beam_cleanup)
