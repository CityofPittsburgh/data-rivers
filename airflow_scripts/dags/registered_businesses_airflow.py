from __future__ import absolute_import

import os

from airflow import DAG
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_year, get_ds_month, default_args, geocode_address_query, build_revegeo_query

dag = DAG(
    'registered_businesses',
    default_args=default_args,
    schedule_interval='@monthly',
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year}
)

registered_businesses_gcs = DockerOperator(
    task_id='registered_businesses_gcs',
    image='gcr.io/data-rivers/pgh-finance',
    api_version='auto',
    auto_remove=True,
    environment={
        'ISAT_UN': os.environ['ISAT_UN'],
        'ISAT_PW': os.environ['ISAT_PW'],
        'GCS_AUTH_FILE': '/root/finance-open-data/data-rivers-service-acct.json'
    },
    dag=dag
)

registered_businesses_dataflow = BashOperator(
    task_id='registered_businesses_dataflow',
    bash_command="python {}dependencies/dataflow_scripts/registered_businesses_dataflow.py --input gs://{}_finance/"
                 .format(os.environ['DAGS_PATH'], os.environ['GCS_PREFIX']) + "{{ ds|get_ds_year }}/{{ ds|get_ds_month "
                 "}}/{{ ds }}_registered_businesses.json --avro_output " + "gs://{}_finance/avro_output/".format(
                 os.environ['GCS_PREFIX']) + "{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ ds }}/",
    dag=dag
)

registered_businesses_bq = GoogleCloudStorageToBigQueryOperator(
    task_id='registered_businesses_bq',
    destination_project_dataset_table='{}:finance.registered_businesses_temp'.format(os.environ['GCLOUD_PROJECT']),
    bucket='{}_finance'.format(os.environ['GCS_PREFIX']),
    source_objects=["finance/avro_output/{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ ds }}/*.avro"],
    write_disposition='WRITE_APPEND',
    source_format='AVRO',
    time_partitioning={'type': 'DAY'},
    dag=dag
)

registered_businesses_revgeo_bq = BigQueryOperator(
    task_id='registered_businesses_revgeo_bq',
    sql=geocode_address_query('finance', 'registered_businesses_temp'),
    destination_dataset_table='{}:finance.registered_businesses_revgeo_temp'.format(os.environ['GCLOUD_PROJECT']),
    time_partitioning={'type': 'DAY'},
    use_legacy_sql=False,
    dag=dag
)

# there are only about 20k businesses and the job is monthly, so fine to just overwrite the table every time

registered_businesses_geo_bq = BigQueryOperator(
    task_id='registered_businesses_geo_bq',
    sql=build_revegeo_query('finance', 'registered_businesses_revgeo_temp'),
    use_legacy_sql=False,
    destination_dataset_table='{}:finance.registered_businesses'.format(os.environ['GCLOUD_PROJECT']),
    write_disposition='WRITE_TRUNCATE',
    time_partitioning={'type': 'DAY'},
    dag=dag
)

registered_businesses_drop_temp = BigQueryOperator(
    task_id='registered_businesses_drop_temp',
    sql='DROP TABLE `{}.finance.registered_businesses_temp`'.format(os.environ['GCLOUD_PROJECT']),
    use_legacy_sql=False,
    dag=dag
)

registered_businesses_drop_revgeo_temp = BigQueryOperator(
    task_id='registered_businesses_drop_temp',
    sql='DROP TABLE `{}.finance.registered_businesses_revgeo_temp`'.format(os.environ['GCLOUD_PROJECT']),
    use_legacy_sql=False,
    dag=dag
)

registered_businesses_beam_cleanup = BashOperator(
    task_id='registered_businesses_beam_cleanup',
    bash_command=airflow_utils.beam_cleanup_statement('{}_finance'.format(os.environ['GCS_PREFIX'])),
    dag=dag
)

registered_businesses_gcs >> registered_businesses_dataflow >> registered_businesses_bq >> \
    registered_businesses_revgeo_bq >> registered_businesses_geo_bq >> (registered_businesses_drop_temp,
                                                                        registered_businesses_drop_revgeo_temp,
                                                                        registered_businesses_beam_cleanup)
