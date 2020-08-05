from __future__ import absolute_import

import os

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

from dependencies import airflow_utils
from dependencies.airflow_utils import build_revgeo_query, get_ds_year, get_ds_month, default_args, dedup_table

# TODO: When Airflow 2.0 is released, upgrade the package, sub in DataFlowPythonOperator for BashOperator,
# and pass the argument 'py_interpreter=python3'

dag = DAG(
    'wprdc_ems_fire',
    default_args=default_args,
    schedule_interval='@monthly',
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year}
)

wprdc_ems_fire_gcs = BashOperator(
    task_id='wprdc_ems_fire_gcs',
    bash_command='python {}'.format(os.environ['DAGS_PATH'] + '/dependencies/gcs_loaders/wprdc_ems_fire_gcs.py '
                                                              '--execution_date {{ ds }}'),
    dag=dag
)

wprdc_ems_dataflow = BashOperator(
    task_id='wprdc_ems_dataflow',
    bash_command="python {}/dependencies/dataflow_scripts/wprdc_ems_dataflow.py --input gs://{}_ems_fire/ems/"
                 .format(os.environ['DAGS_PATH'], os.environ['GCS_PREFIX']) + "{{ ds|get_ds_year }}/"
                 "{{ ds|get_ds_month }}/{{ ds }}_ems.json --avro_output " + "gs://{}_ems_fire/ems/avro_output/"
                 .format(os.environ['GCS_PREFIX']) + "{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ ds }}/",
    dag=dag
)

wprdc_fire_dataflow = BashOperator(
    task_id='wprdc_fire_dataflow',
    bash_command="python {}/dependencies/dataflow_scripts/wprdc_fire_dataflow.py --input gs://{}_ems_fire/"
                 "fire/".format(os.environ['DAGS_PATH'], os.environ['GCS_PREFIX']) + "{{ ds|get_ds_year }}/"
                 "{{ ds|get_ds_month }}/{{ ds }}_fire.json --avro_output " + "gs://{}_ems_fire/fire/"
                 "avro_output/".format(os.environ['GCS_PREFIX']) + "{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ ds "
                                                                   "}}/",
    dag=dag
)

wprdc_ems_bq = GoogleCloudStorageToBigQueryOperator(
    task_id='wprdc_ems_bq',
    destination_project_dataset_table='{}:ems_fire_calls.ems_raw'.format(os.environ['GCLOUD_PROJECT']),
    bucket='{}_ems_fire'.format(os.environ['GCS_PREFIX']),
    source_objects=["ems/avro_output/{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ ds }}/*.avro"],
    write_disposition='WRITE_APPEND',
    create_disposition='CREATE_IF_NEEDED',
    source_format='AVRO',
    autodetect=True,
    time_partitioning={'type': 'DAY'},
    dag=dag
)

wprdc_fire_bq = GoogleCloudStorageToBigQueryOperator(
    task_id='wprdc_fire_bq',
    destination_project_dataset_table='{}:ems_fire_calls.fire_raw'.format(os.environ['GCLOUD_PROJECT']),
    bucket='{}_ems_fire'.format(os.environ['GCS_PREFIX']),
    source_objects=["fire/avro_output/{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ ds }}/*.avro"],
    write_disposition='WRITE_APPEND',
    create_disposition='CREATE_IF_NEEDED',
    source_format='AVRO',
    autodetect=True,
    time_partitioning={'type': 'DAY'},
    dag=dag
)

wprdc_dedup_ems = BigQueryOperator(
    task_id='wprdc_dedup_ems',
    sql=dedup_table('ems_fire_calls', 'ems_raw'),
    use_legacy_sql=False,
    destination_dataset_table='{}:ems_fire_calls.ems_raw'.format(os.environ['GCLOUD_PROJECT']),
    write_disposition='WRITE_TRUNCATE',
    time_partitioning={'type': 'DAY'},
    dag=dag
)

wprdc_dedup_fire = BigQueryOperator(
    task_id='wprdc_dedup_fire',
    sql=dedup_table('ems_fire_calls', 'fire_raw'),
    use_legacy_sql=False,
    destination_dataset_table='{}:ems_fire_calls.fire_raw'.format(os.environ['GCLOUD_PROJECT']),
    write_disposition='WRITE_TRUNCATE',
    time_partitioning={'type': 'DAY'},
    dag=dag
)

wprdc_ems_geojoin = BigQueryOperator(
    task_id='wprdc_ems_geojoin',
    sql=build_revgeo_query('ems_fire_calls', 'ems_raw'),
    use_legacy_sql=False,
    destination_dataset_table='{}:ems_fire_calls.ems_calls'.format(os.environ['GCLOUD_PROJECT']),
    write_disposition='WRITE_TRUNCATE',
    time_partitioning={'type': 'DAY'},
    dag=dag
)

wprdc_fire_geojoin = BigQueryOperator(
    task_id='wprdc_fire_geojoin',
    sql=build_revgeo_query('ems_fire_calls', 'fire_raw'),
    use_legacy_sql=False,
    destination_dataset_table='{}:ems_fire_calls.fire_calls'.format(os.environ['GCLOUD_PROJECT']),
    write_disposition='WRITE_TRUNCATE',
    time_partitioning={'type': 'DAY'},
    dag=dag
)

wprdc_beam_cleanup = BashOperator(
    task_id='wprdc_beam_cleanup',
    bash_command=airflow_utils.beam_cleanup_statement('{}_ems_fire'.format(os.environ['GCS_PREFIX'])),
    dag=dag
)

wprdc_ems_fire_gcs >> wprdc_ems_dataflow >> wprdc_ems_bq >> wprdc_dedup_ems >> (wprdc_ems_geojoin, wprdc_beam_cleanup)

wprdc_ems_fire_gcs >> wprdc_fire_dataflow >> wprdc_fire_bq >> wprdc_dedup_fire >> \
    (wprdc_fire_geojoin, wprdc_beam_cleanup)
