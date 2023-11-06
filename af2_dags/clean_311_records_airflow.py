from __future__ import absolute_import

import os

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_table_delete_operator import BigQueryTableDeleteOperator
from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_month, get_ds_year, default_args
from dependencies.bq_queries import general_queries as g_q
from dependencies.bq_queries.qscend import transform_enrich_requests as t_q

# The goal of this mini-DAG is to fix a recurring issue where 311 ticket data differs
# between their records in all_tickets_current_status and all_linked_requests in BigQuery

dag = DAG(
    'clean_311_records',
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
                          'police_zone', 'fire_zone', 'dpw_streets', 'dpw_enviro',
                          'dpw_parks', 'input_pii_lat', 'input_pii_long',
                          'google_pii_lat', 'google_pii_long', 'input_anon_lat',
                          'input_anon_long', 'google_anon_lat', 'google_anon_long']}]
comp_fields = [{'req_types': ['request_type_name', 'origin']},
               {'geos': ['address_type', 'neighborhood_name', 'council_district',
                         'ward', 'police_zone', 'fire_zone', 'dpw_streets',
                         'dpw_enviro', 'dpw_parks']}]

# create a temporary table with the most up-to-date request types and
# geographic data sourced from all_tickets_current_status where the
# request type, origin, and geo data does not match that found in all_linked_requests
create_unsynced_table = BigQueryOperator(
    task_id='create_unsynced_table',
    sql=g_q.build_sync_staging_table_query(dataset, new_table, upd_table, src_table, is_deduped, upd_id_field,
                                           join_id_field, field_groups, comp_fields),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

# deduplicate all_linked_requests before running update query, otherwise a
# "BadRequest: 400 UPDATE/MERGE must match at most one source row for each target row"
# error can occur
last_upd_field = 'last_action_unix'
query_dedup_old = g_q.build_dedup_old_updates(dataset, upd_table, upd_id_field, last_upd_field)
dedup_old_updates = BigQueryOperator(
    task_id='dedup_old_updates',
    sql=query_dedup_old,
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

update_unsynced_tickets = BigQueryOperator(
    task_id='update_unsynced_tickets',
    sql=t_q.sync_311_updates(dataset, upd_table, upd_id_field, new_table),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

# clean up BQ by removing temporary WPRDC table after it has been converted to a CSV
delete_temp_table = BigQueryTableDeleteOperator(
    task_id="delete_export",
    deletion_dataset_table=f"{os.environ['GCLOUD_PROJECT']}.{dataset}.{new_table}",
    bigquery_conn_id='google_cloud_default',
    dag=dag
)

create_unsynced_table >> dedup_old_updates >> update_unsynced_tickets >> delete_temp_table
