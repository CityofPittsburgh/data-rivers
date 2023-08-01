from __future__ import absolute_import

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from dependencies.airflow_utils import get_ds_month, get_ds_year, get_ds_day, default_args, \
    build_insert_new_records_query

# This DAG will perform an extract and transformation of phone conversation data obtained from a custom Flex Insights
# via the Twilio API. The extracted data will be aggregated and displayed on the Cherwell Dashboard to provide
# insight on Service Desk performance metrics.

COLS_IN_ORDER = """id, date_time, day_of_week, agent, customer_phone, kind, direction, wait_time,
                   talk_time, wrap_up_time, hold_time"""


dag = DAG(
    'twilio_conversations',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2023, 7, 10),
    catchup=False,
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year,
                          'get_ds_day': get_ds_day},
    max_active_runs=1
)

twilio_gcs = BashOperator(
    task_id='twilio_gcs',
    bash_command=f"python {os.environ['PANDAS_ETL_PATH']}/twilio_conversations_etl.py",
    dag=dag
)

insert_new_convos = BigQueryOperator(
    task_id='insert_new_convos',
    sql=build_insert_new_records_query('twilio', 'incoming_conversations', 'flex_insights_conversations', 'id',
                                       COLS_IN_ORDER),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

twilio_gcs >> insert_new_convos
