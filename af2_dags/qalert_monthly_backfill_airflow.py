from __future__ import absolute_import

import os

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_year, get_ds_month, get_ds_day, default_args, \
    build_revgeo_time_bound_query, build_insert_new_records_query, build_format_dedup_query

from dependencies.bq_queries.qscend import integrate_new_requests as q

COLS_IN_ORDER = """id, parent_ticket_id, child_ticket, dept, status_name, status_code, request_type_name, 
request_type_id, origin, pii_comments, anon_comments, pii_private_notes, create_date_est, create_date_utc, 
create_date_unix, last_action_est, last_action_utc, last_action_unix, closed_date_est, closed_date_utc, 
closed_date_unix, pii_street_num, street, cross_street, street_id, cross_street_id, city, pii_input_address, 
pii_input_address AS pii_google_formatted_address, anon_input_address AS anon_google_formatted_address, address_type, 
google_pii_lat, google_pii_long, google_anon_lat, google_anon_long, input_pii_lat, input_pii_long, input_anon_lat, 
input_anon_long"""

ENRICHED_COLS_IN_ORDER = """id, parent_ticket_id, child_ticket, dept, status_name, status_code, request_type_name,
request_type_id, origin, pii_comments, anon_comments, pii_private_notes, create_date_est, create_date_utc,
create_date_unix, last_action_est, last_action_utc, last_action_unix, closed_date_est, closed_date_utc,
closed_date_unix, pii_street_num, street, cross_street, street_id, cross_street_id, city, pii_input_address,
pii_google_formatted_address, anon_google_formatted_address, address_type, neighborhood_name, council_district,
ward, police_zone, fire_zone, dpw_streets, dpw_enviro, dpw_parks, google_pii_lat, google_pii_long, google_anon_lat,
google_anon_long, input_pii_lat, input_pii_long, input_anon_lat, input_anon_long"""

LINKED_COLS_IN_ORDER = """status_name, status_code, dept, 
request_type_name, request_type_id, origin, pii_comments, anon_comments, pii_private_notes, create_date_est, 
create_date_utc, create_date_unix, last_action_est, last_action_utc, last_action_unix, closed_date_est, closed_date_utc, 
closed_date_unix, pii_street_num, street, cross_street, street_id, cross_street_id, city, pii_input_address, 
pii_google_formatted_address, anon_google_formatted_address, address_type, neighborhood_name, 
council_district, ward, police_zone, fire_zone, dpw_streets, dpw_enviro, dpw_parks, google_pii_lat, google_pii_long, 
google_anon_lat, google_anon_long, input_pii_lat, input_pii_long, input_anon_lat, input_anon_long"""

# Backfill DAG will run daily, but will only actually extract/update data if it is the first day of the month
dag = DAG(
    'qalert_monthly_backfill',
    default_args=default_args,
    schedule_interval='@daily',
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year,
                          'get_ds_day': get_ds_day}
)

path = 'requests/backfill/{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ run_id }}'

# Run gcs_loader
gcs_loader = BashOperator(
    task_id='gcs_loader',
    bash_command=F"python {os.environ['DAGS_PATH']}/dependencies/gcs_loaders/qalert_monthly_backfill_gcs.py "
                 F"--output_arg {path}",
    dag=dag
)

# Run dataflow script
py_cmd = f"python {os.environ['DAGS_PATH']}/dependencies/dataflow_scripts/qalert_backfill_dataflow.py"
in_cmd = f" --input gs://{os.environ['GCS_PREFIX']}_qalert/{path}/*_requests.json"
out_cmd = f" --avro_output gs://{os.environ['GCS_PREFIX']}_qalert/{path}/avro_output/"
run_cmd = f" --specify_runner DataflowRunner"
df_cmd_str = py_cmd + in_cmd + out_cmd + run_cmd
dataflow = BashOperator(
    task_id='dataflow',
    bash_command=df_cmd_str,
    dag=dag
)

# Load AVRO data produced by dataflow script into BQ temp table
gcs_to_bq = GoogleCloudStorageToBigQueryOperator(
    task_id='gcs_to_bq',
    bigquery_conn_id='google_cloud_default',
    destination_project_dataset_table=f"{os.environ['GCLOUD_PROJECT']}:qalert.temp_backfill",
    bucket=f"{os.environ['GCS_PREFIX']}_qalert",
    source_objects=[f"{path}/avro_output/*.avro"],
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    source_format='AVRO',
    autodetect=True,
    dag=dag
)

cast_fields = [{'field': 'pii_lat', 'type': 'FLOAT64'},
               {'field': 'pii_long', 'type': 'FLOAT64'},
               {'field': 'anon_lat', 'type': 'FLOAT64'},
               {'field': 'anon_long', 'type': 'FLOAT64'}]
query_format_subset = build_format_dedup_query('qalert', 'temp_backfill_subset', cast_fields, COLS_IN_ORDER)
query_format_subset += f"WHERE id NOT IN (SELECT id FROM `{os.environ['GCLOUD_PROJECT']}.qalert.all_tickets_current_status`)"
format_subset = BigQueryOperator(
    task_id='format_subset',
    sql=query_format_subset,
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

# Query new tickets to determine if they are in the city limits
city_limits = BigQueryOperator(
    task_id='city_limits',
    sql=q.transform_enrich_requests.build_city_limits_query('temp_backfill_subset', 'input_pii_lat', 'input_pii_long'),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)


query_geo_join = build_revgeo_time_bound_query('qalert', 'temp_backfill_subset', 'backfill_enriched',
                                               'create_date_utc', 'input_pii_lat', 'input_pii_long')
geojoin = BigQueryOperator(
    task_id='geojoin',
    sql=query_geo_join,
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

insert_new_parent = BigQueryOperator(
    task_id='insert_new_parent',
    sql=q.integrate_new_requests.insert_new_parent('backfill_enriched', LINKED_COLS_IN_ORDER),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

build_child_ticket_table = BigQueryOperator(
    task_id='build_child_ticket_table',
    sql=q.integrate_new_requests.build_child_ticket_table('backfill_temp_child_combined', 'backfill_enriched',
                                                          combined_children=False),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

increment_ticket_count = BigQueryOperator(
    task_id='increment_ticket_count',
    sql=q.integrate_new_requests.increment_ticket_counts('backfill_temp_child_combined'),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

append_fields = [{'fname': 'child_ids', 'delim': ', '},
                 {'fname': 'anon_comments', 'delim': ' <BREAK> '},
                 {'fname': 'pii_private_notes', 'delim': ' <BREAK> '}]
append_query = ''
for field in append_fields:
    append_query += q.integrate_new_requests.append_to_text_field('backfill_temp_child_combined',
                                                                  field['fname'], field['delim']) + ';'
integrate_children = BigQueryOperator(
    task_id='integrate_children',
    sql=append_query,
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

insert_missed_requests = BigQueryOperator(
    task_id='insert_missed_requests',
    sql=build_insert_new_records_query('qalert', 'backfill_enriched', 'all_tickets_current_status', 'id',
                                       ENRICHED_COLS_IN_ORDER),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

# Clean up
beam_cleanup = BashOperator(
    task_id='qalert_beam_cleanup',
    bash_command=airflow_utils.beam_cleanup_statement(f"{os.environ['GCS_PREFIX']}_qalert"),
    dag=dag
)

# DAG execution:
gcs_loader >> dataflow >> gcs_to_bq >> format_subset >> city_limits >> geojoin >> insert_new_parent >> \
    insert_missed_requests >> beam_cleanup

gcs_loader >> dataflow >> gcs_to_bq >> format_subset >> city_limits >> geojoin >> build_child_ticket_table >>\
    increment_ticket_count >> integrate_children >> insert_missed_requests >> beam_cleanup
