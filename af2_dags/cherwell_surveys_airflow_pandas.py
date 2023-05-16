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
    'cherwell_surveys_pandas',
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
bucket = f"gs://{os.environ['GCS_PREFIX']}_{source}"
hot_bucket = f"{os.environ['GCS_PREFIX']}_hot_metal"
dataset = "surveys"
exec_date = "{{ ds }}"
path = "{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ run_id }}"
json_loc = f"{dataset}/{path}_{dataset}.json"
avro_loc = "survey_responses"
table = "customer_satisfaction_survey_responses_pandas"
id_col = "id"
date_fields = ['created_date_EST', 'created_date_UTC',
               'submitted_date_EST', 'submitted_date_UTC',
               'last_modified_date_EST', 'last_modified_date_UTC']


cherwell_surveys_pandas_gcs = BashOperator(
    task_id='cherwell_surveys_pandas_gcs',
    bash_command=f"python {os.environ['GCS_LOADER_PATH']}/cherwell_surveys_gcs.py --output_arg {json_loc}",
    dag=dag
)

cherwell_surveys_pandas = BashOperator(
    task_id='cherwell_surveys_pandas',
    bash_command=f"python {os.environ['DATAFLOW_SCRIPT_PATH']}/cherwell_surveys_pandas.py "
                 f"--input {json_loc}",
    dag=dag
)

cherwell_surveys_pandas_bq_load = GoogleCloudStorageToBigQueryOperator(
    task_id='cherwell_surveys_pandas_bq_load',
    destination_project_dataset_table=f"{os.environ['GCLOUD_PROJECT']}.{source}.{table}",
    bucket=hot_bucket,
    source_objects=[f"{avro_loc}*.avro"],
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    source_format='AVRO',
    autodetect=True,
    bigquery_conn_id='google_cloud_default',
    dag=dag
)

format_date_query = build_format_dedup_query(source, table, 'DATETIME', date_fields, COLS,
                                             datestring_fmt="%Y-%m-%d %H:%M:%S")
pandas_format_dates = BigQueryOperator(
    task_id='pandas_format_dates',
    sql=format_date_query,
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

pandas_dedup_table = BigQueryOperator(
    task_id='pandas_dedup_table',
    sql=build_dedup_old_updates(source, table, id_col, 'last_modified_date_UNIX'),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

pandas_beam_cleanup = BashOperator(
    task_id='pandas_beam_cleanup',
    bash_command=airflow_utils.beam_cleanup_statement(f"{os.environ['GCS_PREFIX']}_{source}"),
    dag=dag
)

cherwell_surveys_pandas_gcs >> cherwell_surveys_pandas >> cherwell_surveys_pandas_bq_load >> pandas_format_dates >> \
    pandas_dedup_table >> pandas_beam_cleanup
