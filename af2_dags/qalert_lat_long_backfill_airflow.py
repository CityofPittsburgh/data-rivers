from __future__ import absolute_import

import os

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator

from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_year, get_ds_month, default_args, build_city_limits_query, \
    build_revgeo_time_bound_query

COLS_IN_ORDER = """id, parent_ticket_id, child_ticket, pii_input_lat, pii_input_long, 
anon_input_lat, anon_input_long"""

LINKED_COLS_IN_ORDER = """status_name, status_code, dept, 
request_type_name, request_type_id, pii_comments, pii_private_notes, create_date_est, create_date_utc, 
create_date_unix, last_action_est, last_action_unix, last_action_utc, closed_date_est, closed_date_utc, closed_date_unix, 
pii_street_num, street, cross_street, street_id, cross_street_id, city, pii_input_address, 
pii_google_formatted_address, address_type, anon_google_formatted_address, neighborhood_name, 
council_district, ward, police_zone, fire_zone, dpw_streets, dpw_enviro, dpw_parks, pii_lat, pii_long, anon_lat, 
anon_long"""

EXCLUDE_TYPES = """'Hold - 311', 'Graffiti, Owner Refused DPW Removal', 'Medical Exemption - Tote', 
'Snow Angel Volunteer', 'Claim form (Law)','Snow Angel Intake', 'Application Request', 'Reject to 311', 'Referral', 
'Question'"""

SAFE_FIELDS = """status_name, status_code, dept, 
request_type_name, request_type_id, create_date_est, create_date_utc, 
create_date_unix, last_action_est, last_action_unix, last_action_utc, closed_date_est, closed_date_utc,  
closed_date_unix, street, cross_street, street_id, cross_street_id, city, address_type,  
anon_google_formatted_address, neighborhood_name, council_district, ward, police_zone, fire_zone,
dpw_streets, dpw_enviro, dpw_parks, anon_input_lat, anon_input_long, anon_google_lat, anon_google_long"""

# This DAG schedule interval set to None because it will only ever be triggered manually
dag = DAG(
    'qalert_backfill_lat_longs',
    default_args=default_args,
    schedule_interval=None,
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year}
)

# Note: The GCS loader can be run outside of the DAG for testing purposes. (Execution time for 04-2015 throug 11-2021
# backfill was approx 1 hour). Comment/Uncomment as needed.
# Run gcs_loader
gcs_loader = BashOperator(
    task_id='gcs_loader',
    bash_command=F"python {os.environ['DAGS_PATH']}/dependencies/gcs_loaders/qalert_lat_long_backfill_gcs.py",
    dag=dag
)

# Run dataflow_script
py_cmd = f"python {os.environ['DAGS_PATH']}/dependencies/dataflow_scripts/qalert_requests_dataflow.py"
in_cmd = \
    f" --input gs://{os.environ['GCS_PREFIX']}_qalert/requests/backfill/2022-05-03/backfilled_lat_long_requests.json"
out_cmd = f" --avro_output gs://{os.environ['GCS_PREFIX']}_qalert/requests/backfill/2022-05-03/avro_output/"
df_cmd_str = py_cmd + in_cmd + out_cmd
dataflow = BashOperator(
    task_id='dataflow',
    bash_command=df_cmd_str,
    dag=dag
)

# Load AVRO data produced by dataflow_script into BQ temp table
gcs_to_bq = GoogleCloudStorageToBigQueryOperator(
    task_id='gcs_to_bq',
    destination_project_dataset_table=f"{os.environ['GCLOUD_PROJECT']}:qalert.original_lat_longs",
    bucket=f"{os.environ['GCS_PREFIX']}_qalert",
    source_objects=[f"requests/backfill/2022-05-03/*.avro"],
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    source_format='AVRO',
    autodetect=True,
    dag=dag
)

# Update geo coords with lat/long cast as floats (dataflow/AVRO glitch forces them to be output as strings; the
# source of the error is instrinsic to dataflow and may not be fixable). Also, dedupe the results (someties the same
# ticket appears in the computer system more than 1 time (a QAlert glitch)
query_format_dedupe = f"""
CREATE OR REPLACE TABLE `{os.environ['GCLOUD_PROJECT']}.qalert.original_lat_longs` AS
WITH formatted  AS 
    (
    SELECT 
        DISTINCT * EXCEPT (pii_input_lat, pii_input_long, anon_input_lat, anon_input_long),
        CAST(pii_input_lat AS FLOAT64) AS pii_input_lat,
        CAST(pii_input_long AS FLOAT64) AS pii_input_long,
        CAST(anon_input_lat AS FLOAT64) AS anon_input_lat,
        CAST(anon_input_long AS FLOAT64) AS anon_input_long,
    FROM 
        {os.environ['GCLOUD_PROJECT']}.qalert.original_lat_longs
    )
SELECT {COLS_IN_ORDER} FROM formatted
"""
format_dedupe = BigQueryOperator(
    task_id='format_dedupe',
    sql=query_format_dedupe,
    use_legacy_sql=False,
    dag=dag
)

query_join_tables = f"""
CREATE OR REPLACE TABLE `{os.environ['GCLOUD_PROJECT']}.qalert.all_tickets_complete_lat_longs` AS
SELECT 
    group_id, child_ids, num_requests, parent_closed, status_name, 
    status_code, dept, request_type_name, request_type_id, 
    pii_comments, pii_private_notes, create_date_est, create_date_utc, 
    create_date_unix, last_action_est, last_action_utc, last_action_unix, 
    closed_date_est, closed_date_utc, closed_date_unix, pii_street_num, 
    street, cross_street, street_id, cross_street_id, city, 
    pii_input_address, pii_google_formatted_address, anon_google_formatted_address,
    address_type, within_city, neighborhood_name, council_district, ward,
    police_zone, fire_zone, dpw_streets, dpw_enviro, dpw_parks,
    alr.pii_lat AS google_pii_lat, alr.pii_long AS google_pii_long, 
    alr.anon_lat AS gooogle_anon_lat, alr.anon_long AS google_anon_long,
    org.input_pii_lat AS input_pii_lat, org.input_pii_long AS input_pii_long,
    org.input_anon_lat AS input_anon_lat, org.input_anon_long AS input_anon_long
FROM `{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests` alr
JOIN `{os.environ['GCLOUD_PROJECT']}.qalert.original_lat_longs` org
ON alr.group_id = org.id
WHERE org.child_ticket = False
"""
join_lat_longs = BigQueryOperator(
    task_id='join_lat_longs',
    sql=query_join_tables,
    use_legacy_sql=False,
    dag=dag
)

# Create a table from all_tickets_complete_lat_longs that has all columns EXCEPT those that have potential PII. This table is
# subsequently exported to WPRDC. BQ will not currently (2022-05-03) allow data to be pushed from a query and it must
# be stored in a table prior to the push. Thus, this is a 2 step process also involving the operator below.
query_drop_pii = f"""
CREATE OR REPLACE TABLE `{os.environ['GCLOUD_PROJECT']}.qalert.full_lat_long_data_scrubbed` AS
SELECT 
    group_id, 
    child_ids, 
    num_requests, 
    parent_closed,
    {SAFE_FIELDS}
FROM 
    `{os.environ['GCLOUD_PROJECT']}.qalert.all_tickets_complete_lat_longs`
"""
drop_pii_for_export = BigQueryOperator(
    task_id='drop_pii_for_export',
    sql=query_drop_pii,
    use_legacy_sql=False,
    dag=dag
)

# Export table as CSV to WPRDC bucket
wprdc_export = BigQueryToCloudStorageOperator(
    task_id='wprdc_export',
    source_project_dataset_table=f"{os.environ['GCLOUD_PROJECT']}:qalert.full_lat_long_data_scrubbed",
    destination_cloud_storage_uris=[f"gs://{os.environ['GCS_PREFIX']}_wprdc/qalert_lat_longs_backfill_" + "{{ ds }}.csv"],
    dag=dag
)

# Clean up
beam_cleanup = BashOperator(
    task_id='qalert_beam_cleanup',
    bash_command=airflow_utils.beam_cleanup_statement(f"{os.environ['GCS_PREFIX']}_qalert"),
    dag=dag
)

# DAG execution:
gcs_loader >> dataflow >> gcs_to_bq >> format_dedupe >>  \
drop_pii_for_export >> wprdc_export >> beam_cleanup