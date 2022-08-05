from __future__ import absolute_import

import os

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_month, get_ds_year, \
    default_args, build_sync_update_query

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
    bash_command=f"python {os.environ['SQL_SCRIPT_PATH']}/create_unsynced_table.py",
    dag=dag
)

dataset = 'qalert'
upd_table = 'all_linked_requests'
source_table = 'temp_curr_status_merge'
id_field = 'group_id'
upd_fields = ['request_type_name', 'request_type_id', 'pii_street_num', 'street',
              'cross_street', 'street_id',  'cross_street_id', 'city',
              'pii_input_address', 'pii_google_formatted_address', 'origin',
              'address_type', 'anon_google_formatted_address', 'neighborhood_name',
              'council_district', 'ward', 'police_zone', 'fire_zone', 'dpw_streets',
              'dpw_enviro', 'dpw_parks', 'input_pii_lat', 'input_pii_long',
              'input_anon_lat', 'input_anon_long', 'google_pii_lat', 'google_pii_long',
              'google_anon_lat', 'google_anon_long']
query_gender_comp = build_sync_update_query(dataset, upd_table, source_table,
                                            id_field, upd_fields)
update_unsynced_tickets = BigQueryOperator(
        task_id = 'update_unsynced_tickets',
        sql = query_gender_comp,
        bigquery_conn_id='google_cloud_default',
        use_legacy_sql = False,
        dag = dag
)

beam_cleanup = BashOperator(
    task_id='beam_cleanup',
    bash_command=airflow_utils.beam_cleanup_statement('{}_ticket_mismatch_fix'.format(os.environ['GCS_PREFIX'])),
    dag=dag
)

create_unsynced_table >> update_unsynced_tickets >> beam_cleanup