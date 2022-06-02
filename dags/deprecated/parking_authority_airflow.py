from __future__ import absolute_import

import os

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

from deprecated import airflow_utils
from deprecated.airflow_utils import build_revgeo_query, get_ds_year, get_ds_month, default_args

# TODO: When Airflow 2.0 is released, upgrade the package, sub in DataFlowPythonOperator for BashOperator,
# and pass the argument 'py_interpreter=python3'

dag = DAG(
    'parking_authority',
    default_args=default_args,
    schedule_interval='@weekly',
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year}
)

parking_meters_gcs = BashOperator(
    task_id='parking_meters_gcs',
    bash_command='python {}'.format(os.environ['DAGS_PATH'] + '/dependencies/gcs_loaders/parking_authority_gcs.py '
                                                              '--execution_date {{ ds }} --prev_execution_date '
                                                              '{{ prev_ds }}'),
    dag=dag
)

parking_meters_dataflow = BashOperator(
    task_id='parking_meters_dataflow',
    bash_command="python {}/dependencies/dataflow_scripts/parking_meters_dataflow.py --input gs://{}_parking/meters/"
                 .format(os.environ['DAGS_PATH'], os.environ['GCS_PREFIX']) + "{{ ds|get_ds_year }}/"
                 "{{ ds|get_ds_month }}/{{ ds }}_meters.json --avro_output " + "gs://{}_parking/meters/avro_output/"
                 .format(os.environ['GCS_PREFIX']) + "{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ ds }}/",
    dag=dag
)

parking_meters_bq = GoogleCloudStorageToBigQueryOperator(
    task_id='parking_meters_bq',
    destination_project_dataset_table='{}:parking_transactions.meters_raw'.format(os.environ['GCLOUD_PROJECT']),
    bucket='{}_parking'.format(os.environ['GCS_PREFIX']),
    source_objects=["meters/avro_output/{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ ds }}/*.avro"],
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    source_format='AVRO',
    autodetect=True,
    time_partitioning={'type': 'DAY'},
    dag=dag
)

parking_meters_geojoin = BigQueryOperator(
    task_id='parking_meters_geojoin',
    sql=build_revgeo_query('parking_transactions', 'meters_raw', 'id'),
    use_legacy_sql=False,
    destination_dataset_table='{}:parking_transactions.meters'.format(os.environ['GCLOUD_PROJECT']),
    write_disposition='WRITE_TRUNCATE',
    time_partitioning={'type': 'DAY'},
    dag=dag
)

parking_beam_cleanup = BashOperator(
    task_id='parking_beam_cleanup',
    bash_command= airflow_utils.beam_cleanup_statement('{}_parking'.format(os.environ['GCS_PREFIX'])),
    dag=dag
)

parking_transactions_dataflow = BashOperator(
    task_id='parking_transactions_dataflow',
    bash_command="python {}/dependencies/dataflow_scripts/parking_transactions_dataflow"
                 ".py --input gs://{}_parking/transactions/"
                 .format(os.environ['DAGS_PATH'], os.environ['GCS_PREFIX']) + "{{ ds|get_ds_year }}/"
                 "{{ ds|get_ds_month }}/{{ ds }}_transactions.json --avro_output "
                 + "gs://{}_parking/transactions/avro_output/"
                 .format(os.environ['GCS_PREFIX']) + "{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ ds }}/",
    dag=dag
)

parking_transactions_bq = GoogleCloudStorageToBigQueryOperator(
    task_id='parking_transactions_bq',
    destination_project_dataset_table='{}:parking_transactions.payment_raw'.format(os.environ['GCLOUD_PROJECT']),
    bucket='{}_parking'.format(os.environ['GCS_PREFIX']),
    source_objects=["meters/avro_output/{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ ds }}/*.avro"],
    write_disposition='WRITE_APPEND',
    create_disposition='CREATE_IF_NEEDED',
    source_format='AVRO',
    autodetect=True,
    time_partitioning={'type': 'DAY'},
    dag=dag
)

parking_transactions_geojoin = BigQueryOperator(
    task_id='parking_transactions_geojoin',
    sql=build_revgeo_query('parking_transactions', 'payment_raw', 'id'),
    use_legacy_sql=False,
    destination_dataset_table='{}:parking_transactions.payment'.format(
        os.environ['GCLOUD_PROJECT']),
    write_disposition='WRITE_APPEND',
    time_partitioning={'type': 'DAY'},
    dag=dag
)

parking_beam_cleanup = BashOperator(
    task_id='parking_beam_cleanup',
    bash_command= airflow_utils.beam_cleanup_statement('{}_parking'.format(os.environ['GCS_PREFIX'])),
    dag=dag
)

parking_meters_gcs >> parking_meters_dataflow >> parking_meters_bq >> (
    parking_meters_geojoin, parking_beam_cleanup)

parking_meters_gcs >> parking_transactions_dataflow >> \
parking_transactions_bq >> (parking_transactions_geojoin, parking_beam_cleanup)