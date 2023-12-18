# TODO: When Airflow 2.0 is released, upgrade the package, sub in DataFlowPythonOperator for BashOperator,
#  and pass the argument 'py_interpreter=python3'


from __future__ import absolute_import

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator

from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_year, get_ds_month, get_ds_day, default_args

from dependencies.bq_queries.qscend import integrate_new_requests, transform_enrich_requests
from dependencies.bq_queries import geo_queries

INCOMING_COLS = """id, parent_ticket_id, child_ticket, dept, status_name, status_code, request_type_name, 
request_type_id, origin, pii_comments, anon_comments, pii_private_notes, create_date_est, create_date_utc, 
create_date_unix, last_action_est, last_action_utc, last_action_unix, closed_date_est, closed_date_utc, 
closed_date_unix, pii_street_num, street, cross_street, street_id, cross_street_id, city, pii_input_address, 
pii_google_formatted_address, anon_google_formatted_address, address_type, google_pii_lat, google_pii_long, 
google_anon_lat, google_anon_long, input_pii_lat, input_pii_long, input_anon_lat, input_anon_long"""

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

SAFE_FIELDS = """status_name, status_code, dept, request_type_name, request_type_id, origin, create_date_est, 
create_date_utc, create_date_unix, last_action_est, last_action_unix, last_action_utc, closed_date_est, 
closed_date_utc, closed_date_unix, street, cross_street, street_id, cross_street_id, city, 
anon_google_formatted_address, address_type, neighborhood_name, council_district, ward, police_zone, fire_zone, 
dpw_streets, dpw_enviro, dpw_parks"""

PII_COORDS = "google_pii_lat, google_pii_long, input_pii_lat, input_pii_long"

ANON_COORDS = " input_anon_lat, input_anon_long, google_anon_lat, google_anon_long"

dag = DAG(
    'qalert_requests',
    default_args=default_args,
    schedule_interval='@hourly',
    start_date=datetime(2021, 1, 21),
    catchup=False,
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year,
                          'get_ds_day': get_ds_day},
    max_active_runs=1
)

# initialize gcs locations
bucket = f"gs://{os.environ['GCS_PREFIX']}_qalert"
dataset = "requests"

path = "{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ ds|get_ds_day }}/{{ run_id }}"

json_loc = f"{path}_requests.json"
avro_loc = f"avro_output/{path}/"

# Run gcs_loader
exec_gcs = f"python {os.environ['GCS_LOADER_PATH']}/qalert_requests_gcs.py"
gcs_loader = BashOperator(
    task_id='gcs_loader',
    bash_command=f"{exec_gcs} --output_arg {dataset}/{json_loc}",
    execution_timeout=timedelta(minutes=15),
    dag=dag
)

exec_df = f"python {os.environ['DATAFLOW_SCRIPT_PATH']}/qalert_requests_dataflow.py"
dataflow = BashOperator(
    task_id='dataflow',
    bash_command=f"{exec_df} --input {bucket}/{dataset}/{json_loc} --avro_output {bucket}/{dataset}/{avro_loc}",
    dag=dag
)

# Load AVRO data produced by dataflow_script into BQ temp table
gcs_to_bq = GoogleCloudStorageToBigQueryOperator(
    task_id='gcs_to_bq',
    destination_project_dataset_table=f"{os.environ['GCLOUD_PROJECT']}:qalert.incoming_actions",
    bucket=f"{os.environ['GCS_PREFIX']}_qalert",
    source_objects=[f"{dataset}/{avro_loc}*.avro"],
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    source_format='AVRO',
    autodetect=True,
    bigquery_conn_id='google_cloud_default',
    dag=dag
)

# Update geo coords with lat/long cast as floats (dataflow/AVRO glitch forces them to be output as strings; the
# source of the error is instrinsic to dataflow and may not be fixable). Also, dedupe the results (someties the same
# ticket appears in the computer system more than 1 time (a QAlert glitch)
query_format_dedupe = f"""
CREATE OR REPLACE TABLE `{os.environ['GCLOUD_PROJECT']}.qalert.incoming_actions` AS
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
        {os.environ['GCLOUD_PROJECT']}.qalert.incoming_actions
    )
-- drop the final column through slicing the string (-13). final column is added in next query     
SELECT 
    {INCOMING_COLS} 
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

# Query new tickets to determine if they are in the city limits
city_limits = BigQueryOperator(
    task_id='city_limits',
    sql=geo_queries.build_city_limits_query('incoming_actions', 'input_pii_lat', 'input_pii_long'),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

# TODO: investigate (and if necessary fix) the unknown source of duplicates in the geojoin query (see util function
#  for clearer explanation)
# FINAL ENRICHMENT OF NEW DATA
# Join all the geo information (e.g. DPW districts, etc) to the new data
query_geo_join = geo_queries.build_revgeo_time_bound_query('qalert', 'incoming_actions', 'create_date_utc',
                                                           'input_pii_lat', 'input_pii_long', 'incoming_enriched')
geojoin = BigQueryOperator(
    task_id='geojoin',
    sql=query_geo_join,
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

query_insert_new_parent = f"""
/*
This query check that a ticket has never been seen before (checks all_tix_current_status) AND
that the ticket is a parent. Satisfying both conditions means that the ticket needs to be placed in all_linked_requests
There is one catch that is caused by the way tickets are manually linked: This newly recorded request is
labeled as a parent. However, in the future the 311 operators may link this ticket with another
existing parent and it will change into a child ticket. This means the original ticket was actually a "false_parent"
ticket. Future steps in the DAG will handle that possibility, and for this query the only feasible option is to assume
the ticket is correctly labeled.*/

INSERT INTO `{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests`
(
SELECT
    id as group_id,
    "" as child_ids,
    1 as num_requests,
    IF(status_name = "closed", TRUE, FALSE) as parent_closed,
    {LINKED_COLS_IN_ORDER}

FROM
    `{os.environ['GCLOUD_PROJECT']}.qalert.incoming_enriched`
WHERE id NOT IN (SELECT id FROM `{os.environ['GCLOUD_PROJECT']}.qalert.all_tickets_current_status`)
AND child_ticket = False
);
"""
insert_new_parent = BigQueryOperator(
    task_id='insert_new_parent',
    sql=query_insert_new_parent,
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

query_remove_false_parents = f"""
/*
As mentioned in then query_insert_new_parent description:
Sometimes a ticket is initially identified as a parent ticket and then is reclassified as a child
(thus the original ticket was a "false parent"). The original ticket's data must be deleted from all_linked_requests.
Next, the incoming child ticket (corresponding to the false parent ticket) needs to be treated as any other newly
arrived child ticket- which is to say that its identifiers and comments/notes need to be 1) extracted and
2) inserted into the appropriate linkage family in all_linked_requests. This query handles
deletion of the false_parent from all_linked_requests and extraction of the newly identified child ticket's data.
the remove_false_parents query will aggregate this child's information and integrate it into all_linked_requests
along with the other child tickets
*/

-- extract the newly identified child's information for integration in the next query
CREATE OR REPLACE TABLE `{os.environ['GCLOUD_PROJECT']}.qalert.temp_prev_parent_now_child` AS
SELECT 
    id AS fp_id, parent_ticket_id, anon_comments, pii_private_notes
FROM `{os.environ['GCLOUD_PROJECT']}.qalert.incoming_enriched`
WHERE id IN (SELECT 
                group_id
             FROM`{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests`)
AND child_ticket = TRUE ;

-- delete the false parent ticket's information 
DELETE FROM `{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests`
WHERE group_id IN 
        (SELECT fp_id FROM `{os.environ['GCLOUD_PROJECT']}.qalert.temp_prev_parent_now_child`);
"""
remove_false_parents = BigQueryOperator(
    task_id='remove_false_parents',
    sql=query_remove_false_parents,
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)


append_fields = [{'fname': 'child_ids', 'delim': ', '},
                 {'fname': 'anon_comments', 'delim': ' <BREAK> '},
                 {'fname': 'pii_private_notes', 'delim': ' <BREAK> '}]
query_integrate_children = F"""
{integrate_new_requests.build_child_ticket_table('temp_child_combined', 'incoming_enriched')};
{integrate_new_requests.increment_ticket_counts('temp_child_combined')};
"""
for field in append_fields:
    query_integrate_children += integrate_new_requests.append_to_text_field('temp_child_combined',
                                                                            field['fname'], field['delim']) + ';'
integrate_children = BigQueryOperator(
    task_id='integrate_children',
    sql=query_integrate_children,
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)


update_cols = ['status_name', 'status_code', 'request_type_name', 'request_type_id',
               'closed_date_est', 'closed_date_utc', 'closed_date_unix',
               'last_action_est', 'last_action_utc', 'last_action_unix']
replace_last_update = BigQueryOperator(
    task_id='replace_last_update',
    sql=integrate_new_requests.replace_last_update('incoming_enriched', update_cols),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)


delete_old_insert_new_records = BigQueryOperator(
    task_id='delete_old_insert_new_records',
    sql=integrate_new_requests.delete_old_insert_new(COLS_IN_ORDER, 'incoming_enriched'),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

# Create a table from all_linked_requests that has all columns EXCEPT those that have potential PII. This table is
# subsequently exported to WPRDC. BQ will not currently (2021-10-01) allow data to be pushed from a query and it must
# be stored in a table prior to the push. Thus, this is a 2 step process also involving the operator below.
drop_pii_for_export = BigQueryOperator(
    task_id='drop_pii_for_export',
    sql=transform_enrich_requests.drop_pii((F"{SAFE_FIELDS}, {PII_COORDS}"), PRIVATE_TYPES),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

# Export table as CSV to WPRDC bucket
wprdc_export = BigQueryToCloudStorageOperator(
    task_id='wprdc_export',
    source_project_dataset_table=f"{os.environ['GCLOUD_PROJECT']}.qalert.data_export_scrubbed",
    destination_cloud_storage_uris=[f"gs://{os.environ['GCS_PREFIX']}_wprdc/qalert_requests/{path}.csv"],
    bigquery_conn_id='google_cloud_default',
    dag=dag
)

# Clean up
beam_cleanup = BashOperator(
    task_id='qalert_beam_cleanup',
    bash_command=airflow_utils.beam_cleanup_statement(f"{os.environ['GCS_PREFIX']}_qalert"),
    dag=dag
)

# DAG execution:
gcs_loader >> dataflow >> gcs_to_bq >> format_dedupe >> city_limits >> geojoin >> insert_new_parent >> \
        remove_false_parents >> integrate_children >> replace_last_update >> delete_old_insert_new_records >> \
        drop_pii_for_export >> wprdc_export >> beam_cleanup
