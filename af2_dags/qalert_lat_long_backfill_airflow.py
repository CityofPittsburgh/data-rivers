from __future__ import absolute_import

import os

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator

from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_year, get_ds_month, get_ds_day, default_args

COLS_IN_ORDER = """id, parent_ticket_id, child_ticket, input_pii_lat, input_pii_long, 
input_anon_lat, input_anon_long"""


# This DAG schedule interval set to None because it will only ever be triggered manually
dag = DAG(
    'qalert_backfill_lat_longs',
    default_args=default_args,
    schedule_interval=None,
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year,
                          'get_ds_day': get_ds_day}
)

path = "{{ ds|get_ds_year }}-{{ ds|get_ds_month }}-{{ ds|get_ds_day }}"

# Note: The GCS loader can be run outside of the DAG for testing purposes. (Execution time for 04-2015 throug 11-2021
# backfill was approx 1 hour). Comment/Uncomment as needed.
# Run gcs_loader
gcs_loader = BashOperator(
    task_id='gcs_loader',
    bash_command=F"python {os.environ['DAGS_PATH']}/dependencies/gcs_loaders/qalert_lat_long_backfill_gcs.py",
    dag=dag
)

# Run dataflow_script
py_cmd = f"python {os.environ['DAGS_PATH']}/dependencies/dataflow_scripts/qalert_lat_long_backfill_dataflow.py"
in_cmd = \
    f" --input gs://{os.environ['GCS_PREFIX']}_qalert/requests/backfill/lat_long_backfill/{path}/backfilled_lat_long_requests.json"
out_cmd = f" --avro_output gs://{os.environ['GCS_PREFIX']}_qalert/requests/backfill/lat_long_backfill/{path}/avro_output/"
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
    destination_project_dataset_table=f"{os.environ['GCLOUD_PROJECT']}:qalert.original_lat_longs",
    bucket=f"{os.environ['GCS_PREFIX']}_qalert",
    source_objects=[f"requests/backfill/lat_long_backfill/{path}/avro_output/*.avro"],
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    source_format='AVRO',
    autodetect=True,
    use_legacy_sql=False,
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
        DISTINCT * EXCEPT (input_pii_lat, input_pii_long, input_anon_lat, input_anon_long),
        CAST(input_pii_lat AS FLOAT64) AS input_pii_lat,
        CAST(input_pii_long AS FLOAT64) AS input_pii_long,
        CAST(input_anon_lat AS FLOAT64) AS input_anon_lat,
        CAST(input_anon_long AS FLOAT64) AS input_anon_long,
    FROM 
        {os.environ['GCLOUD_PROJECT']}.qalert.original_lat_longs
    )
SELECT {COLS_IN_ORDER} FROM formatted
"""
format_dedupe = BigQueryOperator(
    task_id='format_dedupe',
    sql=query_format_dedupe,
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

query_join_tables = f"""
CREATE OR REPLACE TABLE `{os.environ['GCLOUD_PROJECT']}.qalert.all_tickets_complete_lat_longs` AS
SELECT 
    group_id, child_ids, num_requests, parent_closed, status_name, 
    status_code, dept, request_type_name, request_type_id, origin,
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
gcs_loader >> dataflow >> gcs_to_bq >> format_dedupe >> join_lat_longs >> beam_cleanup