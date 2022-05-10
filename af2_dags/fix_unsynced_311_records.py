from __future__ import absolute_import

import os

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_month, get_ds_year, default_args

# The goal of this mini-DAG is to fix a recurring issue where 311 ticket data differs
# between their records in all_tickets_current_status and all_linked_requests in BigQuery

dag = DAG(
    'fix_unsynced_311_records',
    default_args=default_args,
    schedule_interval='@daily',
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year}
)

create_unsynced_table = BashOperator(
    task_id='create_unsynced_table',
    bash_command=f"python {os.environ['DAGS_PATH']}/sql_scripts/create_unsynced_table.py",
    dag=dag
)

update_unsynced_tickets = BashOperator(
    task_id='update_unsynced_tickets',
    bash_command=f"python {os.environ['DAGS_PATH']}/sql_scripts/update_unsynced_tickets.py",
    dag=dag
)

beam_cleanup = BashOperator(
    task_id='beam_cleanup',
    bash_command=airflow_utils.beam_cleanup_statement('{}_ticket_mismatch_fix'.format(os.environ['GCS_PREFIX'])),
    dag=dag
)

create_unsynced_table >> update_unsynced_tickets >> beam_cleanup