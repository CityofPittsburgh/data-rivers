from __future__ import absolute_import

import os

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_month, get_ds_year, default_args_today

# The goal of this mini-DAG is to perform a daily pull of the previous day's weather in Pittsburgh
# using the OpenWeatherMap API. This weather data will be used to provide context to pothole fill
# response times, as rain or other inclement weather could result in a delay to DPW service.

dag = DAG(
    'prev_day_weather',
    default_args=default_args_today,
    schedule_interval='@daily',
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year}
)

# initialize gcs locations
dataset = "weather"
bucket = f"{os.environ['GCS_PREFIX']}_{dataset}"
path = "{{ ds|get_ds_year }}/{{ ds|get_ds_month }}"

prev_day_weather_gcs = BashOperator(
    task_id='prev_day_weather_gcs',
    bash_command=f"python {os.environ['GCS_LOADER_PATH']}"
                 "/prev_day_weather_gcs.py --lookback_date {{ prev_ds }}",
    dag=dag
)

prev_day_weather_bq_load = GoogleCloudStorageToBigQueryOperator(
        task_id = 'prev_day_weather_bq_load',
        destination_project_dataset_table = f"{os.environ['GCLOUD_PROJECT']}.{dataset}.daily_weather",
        bucket = bucket,
        source_objects = [f"{dataset}/{path}/"+"{{ prev_ds }}_weather_report.avro"],
        bigquery_conn_id='google_cloud_default',
        write_disposition='WRITE_APPEND',
        create_disposition='CREATE_IF_NEEDED',
        source_format = 'AVRO',
        autodetect = True,
        dag = dag
)

beam_cleanup = BashOperator(
        task_id = 'prev_day_weather_beam_cleanup',
        bash_command = airflow_utils.beam_cleanup_statement(f"{os.environ['GCS_PREFIX']}_prev_day_weather"),
        dag = dag
)

prev_day_weather_gcs >> prev_day_weather_bq_load >> beam_cleanup