from __future__ import absolute_import

import os


from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from dependencies import airflow_utils
from dependencies.airflow_utils import get_prev_ds_month, get_prev_ds_year, default_args

import pendulum
import pytz

# The goal of this mini-DAG is to perform a daily pull of the previous day's weather in Pittsburgh
# using the OpenWeatherMap API. This weather data will be used to provide context to pothole fill
# response times, as rain or other inclement weather could result in a delay to DPW service.

dag = DAG(
    'prev_day_weather',
    default_args=default_args,
    start_date=pendulum.datetime(2022, 9, 23, 0, 1, tz=pytz.timezone('US/Eastern')),
    schedule_interval='0 0 * * *',
    user_defined_filters={'get_prev_ds_month': get_prev_ds_month, 'get_prev_ds_year': get_prev_ds_year}
)

# initialize gcs locations
dataset = "weather"
bucket = f"{os.environ['GCS_PREFIX']}_{dataset}"
path = "prev_day_weather/{{ prev_ds|get_prev_ds_year }}/{{ prev_ds|get_prev_ds_month }}"

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
        source_objects = [f"{path}/"+"{{ prev_ds }}_weather_report.avro"],
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