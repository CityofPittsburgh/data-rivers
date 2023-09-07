from __future__ import absolute_import

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_month, get_ds_year, get_ds_day, default_args, \
    build_dedup_old_updates, build_format_dedup_query

# This DAG will perform a daily pull of incoming Cherwell Customer Satisfaction Survey results.
# Ticket information will be displayed on a Google Looker Studio dashboard for use by team managers
# that do not have access to Cherwell's admin portal.

COLS = """id, incident_id, created_date_EST, created_date_UTC, created_date_UNIX, submitted_by, 
          submitted_date_EST, submitted_date_UTC, submitted_date_UNIX, survey_complete, q1_timely_resolution, 
          q2_handled_professionally, q3_clear_communication, q4_overall_satisfaction, q5_request_handled_first_time, 
          q6_improvement_suggestions, q7_contact_me, q8_additional_comments, survey_score, avg_survey_score,
          owned_by, last_modified_date_EST, last_modified_date_UTC, last_modified_date_UNIX, last_modified_by"""


dag = DAG(
    'cherwell_surveys',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2023, 5, 8),
    catchup=False,
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year,
                          'get_ds_day': get_ds_day},
    max_active_runs=1
)

# initialize gcs locations and store endpoint names in variables
source = "cherwell"
dataset = "surveys"
path = "{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ run_id }}"
json_loc = f"{dataset}/{path}_{dataset}.json"
table = "customer_satisfaction_survey_responses"
id_col = "id"
cast_fields = [{'field': 'created_date_EST', 'type': 'DATETIME'},
               {'field': 'created_date_UTC', 'type': 'DATETIME'},
               {'field': 'submitted_date_EST', 'type': 'DATETIME'},
               {'field': 'submitted_date_UTC', 'type': 'DATETIME'},
               {'field': 'submitted_date_UNIX', 'type': 'INT64'},
               {'field': 'last_modified_date_EST', 'type': 'DATETIME'},
               {'field': 'last_modified_date_UTC', 'type': 'DATETIME'}]


cherwell_surveys_gcs = BashOperator(
    task_id='cherwell_surveys_gcs',
    bash_command=f"python {os.environ['GCS_LOADER_PATH']}/cherwell_surveys_gcs.py --output_arg {json_loc}",
    dag=dag
)

cherwell_surveys_pandas = BashOperator(
    task_id='cherwell_surveys_pandas',
    bash_command=f"python {os.environ['PANDAS_ETL_PATH']}/cherwell_surveys_pandas.py --input {json_loc}",
    dag=dag
)

format_column_query = build_format_dedup_query(source, table, table, cast_fields, COLS,
                                               datestring_fmt="%Y-%m-%d %H:%M:%S")
format_column_types = BigQueryOperator(
    task_id='format_column_types',
    sql=format_column_query,
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

dedup_table = BigQueryOperator(
    task_id='dedup_table',
    sql=build_dedup_old_updates(source, table, id_col, 'last_modified_date_UNIX'),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

cherwell_surveys_gcs >> cherwell_surveys_pandas >> format_column_types >> dedup_table
