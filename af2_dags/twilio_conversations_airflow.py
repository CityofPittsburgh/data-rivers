from __future__ import absolute_import

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.operators.bigquery_table_delete_operator import BigQueryTableDeleteOperator
from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_month, get_ds_year, get_ds_day, default_args

# This DAG will perform an extract and transformation of phone conversation data obtained from a custom Flex Insights
# via the Twilio API. The extracted data will be aggregated and displayed on the Cherwell Dashboard to provide
# insight on Service Desk performance metrics.

dag = DAG(
    'twilio_conversations',
    default_args=default_args,
    schedule_interval='@hourly',
    start_date=datetime(2023, 7, 10),
    catchup=False,
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year,
                          'get_ds_day': get_ds_day},
    max_active_runs=1
)

twilio_gcs = BashOperator(
    task_id='twilio_gcs',
    bash_command=f"python {os.environ['GCS_LOADER_PATH']}/twilio_conversations_extract.py --output_arg",
    dag=dag
)

twilio_gcs
