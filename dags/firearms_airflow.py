from __future__ import absolute_import

import os

from airflow import DAG
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from dependencies import airflow_utils
from dependencies.airflow_utils import build_revgeo_query, get_ds_month, get_ds_year, default_args

# TODO: When Airflow 2.0 is released, upgrade the package, upgrade the virtualenv to Python3,
# and add the arg py_interpreter='python3' to DataFlowPythonOperator

dag = DAG(
    'firearm_seizures',
    default_args=default_args,
    schedule_interval='@monthly',
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year}
)

gcs_load = DockerOperator(
    task_id='firearms_gcs_docker',
    image='gcr.io/data-rivers/pgh-firearms',
    api_version='auto',
    auto_remove=True,
    environment={
        'RMSPROD_UN': os.environ['RMSPROD_UN'],
        'RMSPROD_PW': os.environ['RMSPROD_PW'],
        'GCS_AUTH_FILE': '/root/firearm-seizures-report/data-rivers-service-acct.json',
        'GCS_PREFIX': os.environ['GCS_PREFIX'],
        'execution_date': '{{ ds }}',
        'execution_month': '{{ ds|get_ds_year }}' + '/' + '{{ ds|get_ds_month }}'
    },
    dag=dag
)

# dataflow_task = DataFlowPythonOperator(
#     task_id='firearms_dataflow',
#     job_name='firearms-dataflow',
#     py_file=os.getcwd() + '/airflow_scripts/dags/dependencies/dataflow_scripts/firearms_dataflow.py'),
#     dag=dag
# )

dataflow_task = BashOperator(
    task_id='firearms_dataflow',
    bash_command="python {}/dependencies/dataflow_scripts/firearms_dataflow.py --input gs://{}_firearm_seizures/"
                 .format(os.environ['DAGS_PATH'], os.environ['GCS_PREFIX']) + "{{ ds|get_ds_year }}/{{ ds|get_ds_month "
                 "}}/{{ ds }}_firearm_seizures.csv --avro_output " + "gs://{}_firearm_seizures/avro_output/"
                 .format(os.environ['GCS_PREFIX']) + "{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ ds }}/",
    dag=dag
)

bq_insert = GoogleCloudStorageToBigQueryOperator(
    task_id='firearms_bq_insert',
    destination_project_dataset_table='{}:firearm_seizures.seizures_raw'.format(os.environ['GCLOUD_PROJECT']),
    bucket='{}_firearm_seizures'.format(os.environ['GCS_PREFIX']),
    source_objects=["avro_output/{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ ds }}/*.avro"],
    write_disposition='WRITE_APPEND',
    source_format='AVRO',
    autodetect=True,
    time_partitioning={'type': 'DAY'},
    dag=dag
)

bq_geojoin = BigQueryOperator(
    task_id='qalert_geojoin',
    sql=build_revgeo_query('firearm_seizures', 'seizures_raw', 'address'),
    use_legacy_sql=False,
    destination_dataset_table='{}:firearm_seizures.seizures'.format(os.environ['GCLOUD_PROJECT']),
    write_disposition='WRITE_APPEND',
    time_partitioning={'type': 'DAY'},
    dag=dag
)

beam_cleanup = BashOperator(
    task_id='firearms_beam_cleanup',
    bash_command=airflow_utils.beam_cleanup_statement('{}_firearm_seizures'.format(os.environ['GCS_PREFIX'])),
    dag=dag
)

gcs_load >> dataflow_task >> (bq_insert, beam_cleanup) >> bq_geojoin
