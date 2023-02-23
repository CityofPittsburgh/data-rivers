from __future__ import absolute_import

import os

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator

from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_year, get_ds_month, get_ds_day, default_args, build_city_limits_query, \
    build_sync_update_query, build_insert_new_records_query

COLS_IN_ORDER = """id, parent_ticket_id, child_ticket, dept, status_name, status_code, request_type_name, 
request_type_id, origin, pii_comments, anon_comments, pii_private_notes, create_date_est, create_date_utc, 
create_date_unix, last_action_est, last_action_utc, last_action_unix, closed_date_est, closed_date_utc, 
closed_date_unix, pii_street_num, street, cross_street, street_id, cross_street_id, city, pii_input_address, 
pii_google_formatted_address, anon_google_formatted_address, address_type, neighborhood_name, 
council_district, ward, police_zone, fire_zone, dpw_streets, dpw_enviro, dpw_parks, google_pii_lat, google_pii_long, 
google_anon_lat, google_anon_long, input_pii_lat, input_pii_long, input_anon_lat, input_anon_long"""

LINKED_COLS_IN_ORDER = """status_name, status_code, dept, 
request_type_name, request_type_id, origin, pii_comments, anon_comments, pii_private_notes, create_date_est, 
create_date_utc, create_date_unix, last_action_est, last_action_utc, last_action_unix, closed_date_est, closed_date_utc, 
closed_date_unix, pii_street_num, street, cross_street, street_id, cross_street_id, city, pii_input_address, 
pii_google_formatted_address, anon_google_formatted_address, address_type, neighborhood_name, 
council_district, ward, police_zone, fire_zone, dpw_streets, dpw_enviro, dpw_parks, google_pii_lat, google_pii_long, 
google_anon_lat, google_anon_long, input_pii_lat, input_pii_long, input_anon_lat, input_anon_long"""

PRIVATE_TYPES = """'Hold - 311', 'Graffiti, Owner Refused DPW Removal', 'Medical Exemption - Tote', 
'Snow Angel Volunteer', 'Claim form (Law)','Snow Angel Intake', 'Application Request', 'Reject to 311', 'Referral', 
'Question'"""

# STEPS TO TAKE BEFORE EXECUTING DAG:
# 1.) Make backups of all_linked_requests and all_tickets_current_status in case something goes wrong

# This DAG schedule interval set to None because it will only ever be triggered manually
dag = DAG(
    'qalert_backfill',
    default_args=default_args,
    schedule_interval=None,
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year,
                          'get_ds_day': get_ds_day}
)

path = "{{ ds|get_ds_year }}-{{ ds|get_ds_month }}-{{ ds|get_ds_day }}"

# Run gcs_loader
gcs_loader = BashOperator(
    task_id='gcs_loader',
    bash_command=F"python {os.environ['DAGS_PATH']}/dependencies/gcs_loaders/qalert_backfill_gcs.py",
    dag=dag
)

# Run dataflow_script
py_cmd = f"python {os.environ['DAGS_PATH']}/dependencies/dataflow_scripts/qalert_backfill_dataflow.py"
in_cmd = \
    f" --input gs://{os.environ['GCS_PREFIX']}_qalert/requests/backfill/{path}/backfilled_requests.json"
out_cmd = f" --avro_output gs://{os.environ['GCS_PREFIX']}_qalert/requests/backfill/{path}/avro_output/"
df_cmd_str = py_cmd + in_cmd + out_cmd
dataflow = BashOperator(
    task_id='dataflow',
    bash_command=df_cmd_str,
    dag=dag
)

# Load AVRO data produced by dataflow_script into BQ temp table
gcs_to_bq = GoogleCloudStorageToBigQueryOperator(
    task_id='gcs_to_bq',
    bigquery_conn_id='google_cloud_default',
    destination_project_dataset_table=f"{os.environ['GCLOUD_PROJECT']}:qalert.temp_backfill",
    bucket=f"{os.environ['GCS_PREFIX']}_qalert",
    source_objects=[f"requests/backfill/{path}/avro_output/*.avro"],
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    source_format='AVRO',
    autodetect=True,
    dag=dag
)

query_format_dedupe = f"""
CREATE OR REPLACE TABLE `{os.environ['GCLOUD_PROJECT']}.qalert.temp_backfill` AS
WITH formatted  AS 
    (
    SELECT 
        DISTINCT * EXCEPT (input_pii_lat, input_pii_long, google_pii_lat, google_pii_long, 
                           input_anon_lat, input_anon_long, google_anon_lat, google_anon_long),
        CAST(input_pii_lat AS FLOAT64) AS input_pii_lat,
        CAST(input_pii_long AS FLOAT64) AS input_pii_long,
        CAST(google_pii_lat AS FLOAT64) AS google_pii_lat,
        CAST(google_pii_long AS FLOAT64) AS google_pii_long,
        CAST(input_anon_lat AS FLOAT64) AS input_anon_lat,
        CAST(input_anon_long AS FLOAT64) AS input_anon_long,
        CAST(google_anon_lat AS FLOAT64) AS google_anon_lat,
        CAST(google_anon_long AS FLOAT64) AS google_anon_long,
    FROM 
        {os.environ['GCLOUD_PROJECT']}.qalert.temp_backfill
    )
-- drop the final column through slicing the string. final column is added in next query     
SELECT 
    {COLS_IN_ORDER} 
FROM 
    formatted
"""
format_dedupe = BigQueryOperator(
    task_id='format_dedupe',
    sql=query_format_dedupe,
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

# Build a subset table containing all records that have problematic request or address types to execute
# the city limits query on (the query is very resource intensive and crashes when ran on too much data).
# The idea is that these records are most likely to be joined into the production table, so they are the
# ones that would be most beneficial to have accurate address info.
query_create_subset = f"""
CREATE OR REPLACE TABLE `{os.environ['GCLOUD_PROJECT']}.qalert.temp_backfill_subset` AS
SELECT * FROM `{os.environ['GCLOUD_PROJECT']}.qalert.temp_backfill`
WHERE address_type = 'Coordinates Only'
OR request_type_name IN ({PRIVATE_TYPES})
"""
create_subset = BigQueryOperator(
    task_id='create_subset',
    sql=query_create_subset,
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

# Query new tickets to determine if they are in the city limits
query_city_lim = build_city_limits_query('qalert', 'temp_backfill_subset', 'input_pii_lat', 'input_pii_long')
city_limits = BigQueryOperator(
    task_id='city_limits',
    sql=query_city_lim,
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

upd_fields = ['address_type']
query_sync_update = build_sync_update_query('qalert', 'temp_backfll', 'temp_backfill_subset', 'id', upd_fields)
update_address_types = BigQueryOperator(
    task_id='update_address_types',
    sql=query_sync_update,
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

insert_missed_tickets = BigQueryOperator(
        task_id='insert_missed_tickets',
        sql=build_insert_new_records_query('qalert', 'temp_backfll', 'all_tickets_current_status', 'id', COLS_IN_ORDER),
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
gcs_loader >> dataflow >> gcs_to_bq >> format_dedupe >> create_subset >> city_limits >> update_address_types >> \
    insert_missed_tickets >> beam_cleanup
