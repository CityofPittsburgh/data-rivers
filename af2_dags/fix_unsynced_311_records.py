from __future__ import absolute_import

import os

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_month, get_ds_year, \
    default_args, build_sync_staging_table_query, build_sync_update_query, build_dedup_old_updates

# The goal of this mini-DAG is to fix a recurring issue where 311 ticket data differs
# between their records in all_tickets_current_status and all_linked_requests in BigQuery

dag = DAG(
    'fix_unsynced_311_records',
    default_args=default_args,
    schedule_interval='@daily',
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year}
)

dataset = 'qalert'
new_table = 'temp_curr_status_merge'
upd_table = 'all_linked_requests'
src_table = 'all_tickets_current_status'
is_deduped = True
upd_id_field = 'group_id'
join_id_field = 'id'
field_groups = [{'req_types': ['request_type_name', 'request_type_id', 'origin']},
                {'geos': ['pii_street_num', 'street', 'cross_street',
                          'street_id', 'cross_street_id', 'city',
                          'pii_input_address', 'pii_google_formatted_address',
                          'anon_google_formatted_address', 'address_type',
                          'neighborhood_name', 'council_district', 'ward',
                          'police_zone', 'fire_zone', 'dpw_streets',  'dpw_enviro',
                          'dpw_parks', 'input_pii_lat', 'input_pii_long',
                          'google_pii_lat', 'google_pii_long', 'input_anon_lat',
                          'input_anon_long', 'google_anon_lat', 'google_anon_long']}]
comp_fields = [{'req_types': ['request_type_name', 'origin']},
                {'geos': ['address_type', 'neighborhood_name', 'council_district',
                          'ward', 'police_zone', 'fire_zone', 'dpw_streets',
                          'dpw_enviro', 'dpw_parks']}]
query_staging_table = build_sync_staging_table_query(dataset, new_table, upd_table,
                                                     src_table, is_deduped, upd_id_field,
                                                     join_id_field, field_groups, comp_fields)

# create a temporary table with the most up-to-date request types and
# geographic data sourced from all_tickets_current_status where the
# request type, origin, and geo data does not match that found in all_linked_requests
create_unsynced_table = BigQueryOperator(
        task_id = 'create_unsynced_table',
        sql = query_staging_table,
        bigquery_conn_id='google_cloud_default',
        use_legacy_sql = False,
        dag = dag
)

upd_fields = ['request_type_name', 'request_type_id', 'pii_street_num', 'street',
              'cross_street', 'street_id',  'cross_street_id', 'city',
              'pii_input_address', 'pii_google_formatted_address', 'origin',
              'address_type', 'anon_google_formatted_address', 'neighborhood_name',
              'council_district', 'ward', 'police_zone', 'fire_zone', 'dpw_streets',
              'dpw_enviro', 'dpw_parks', 'input_pii_lat', 'input_pii_long',
              'input_anon_lat', 'input_anon_long', 'google_pii_lat', 'google_pii_long',
              'google_anon_lat', 'google_anon_long']
query_sync_update = build_sync_update_query(dataset, upd_table, new_table,
                                            upd_id_field, upd_fields)
update_unsynced_tickets = BigQueryOperator(
        task_id = 'update_unsynced_tickets',
        sql = query_sync_update,
        bigquery_conn_id='google_cloud_default',
        use_legacy_sql = False,
        dag = dag
)

last_upd_field = 'last_action_unix'
query_dedup_old = build_dedup_old_updates(dataset, upd_table,
                                          upd_id_field, last_upd_field)
dedup_old_updates = BigQueryOperator(
    task_id='dedup_old_updates',
    sql=query_dedup_old,
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

beam_cleanup = BashOperator(
    task_id='beam_cleanup',
    bash_command=airflow_utils.beam_cleanup_statement(f"{os.environ['GCS_PREFIX']}_fix_unsynced_311_records"),
    dag=dag
)

create_unsynced_table >> update_unsynced_tickets >> dedup_old_updates >> beam_cleanup