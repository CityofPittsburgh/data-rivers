from __future__ import absolute_import

import os

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_month, get_ds_year, \
    default_args, build_sync_staging_table_query, build_sync_update_query, build_dedup_old_updates

# The goal of this mini-DAG is to fix a recurring issue where 311 ticket data differs
# between their records in all_tickets_current_status and all_linked_requests in BigQuery

dag = DAG(
    'clean_311_records',
    default_args=default_args,
    schedule_interval='@daily',
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year}
)

def init_cmds_xcomm(**kwargs):
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
    kwargs['ti'].xcom_push(key="query_staging_table", value=query_staging_table)
    upd_fields = ['request_type_name', 'request_type_id', 'pii_street_num', 'street',
                  'cross_street', 'street_id', 'cross_street_id', 'city',
                  'pii_input_address', 'pii_google_formatted_address', 'origin',
                  'address_type', 'anon_google_formatted_address', 'neighborhood_name',
                  'council_district', 'ward', 'police_zone', 'fire_zone', 'dpw_streets',
                  'dpw_enviro', 'dpw_parks', 'input_pii_lat', 'input_pii_long',
                  'input_anon_lat', 'input_anon_long', 'google_pii_lat', 'google_pii_long',
                  'google_anon_lat', 'google_anon_long']
    query_sync_update = build_sync_update_query(dataset, upd_table, new_table, upd_id_field, upd_fields)
    kwargs['ti'].xcom_push(key="query_sync_update", value=query_sync_update)
    last_upd_field = 'last_action_unix'
    query_dedup_old = build_dedup_old_updates(dataset, upd_table, upd_id_field, last_upd_field)
    kwargs['ti'].xcom_push(key="query_dedup_old", value=query_dedup_old)


push_xcom = PythonOperator(
        task_id='push_xcom',
        python_callable=init_cmds_xcomm,
        dag=dag
)

# create a temporary table with the most up-to-date request types and
# geographic data sourced from all_tickets_current_status where the
# request type, origin, and geo data does not match that found in all_linked_requests
create_unsynced_table = BigQueryOperator(
        task_id='create_unsynced_table',
        sql=str("{{ ti.xcom_pull(task_ids='push_xcom', key='query_staging_table') }}"),
        bigquery_conn_id='google_cloud_default',
        use_legacy_sql=False,
        dag=dag
)

update_unsynced_tickets = BigQueryOperator(
        task_id='update_unsynced_tickets',
        sql=str("{{ ti.xcom_pull(task_ids='push_xcom', key='query_sync_update') }}"),
        bigquery_conn_id='google_cloud_default',
        use_legacy_sql=False,
        dag=dag
)

dedup_old_updates = BigQueryOperator(
    task_id='dedup_old_updates',
    sql=str("{{ ti.xcom_pull(task_ids='push_xcom', key='query_dedup_old') }}"),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)


push_xcom >> create_unsynced_table >> update_unsynced_tickets >> dedup_old_updates