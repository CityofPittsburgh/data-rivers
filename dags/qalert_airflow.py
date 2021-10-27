from __future__ import absolute_import

import os

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.operators.bigquery_table_delete_operator import BigQueryTableDeleteOperator

from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_year, get_ds_month, default_args, \
    format_gcs_call, format_dataflow_call, build_city_limits_query, build_revgeo_time_bound_query


# TODO: When Airflow 2.0 is released, upgrade the package, sub in DataFlowPythonOperator for BashOperator,
#  and pass the argument 'py_interpreter=python3'


PII_FIELDS = """pii_comments, pii_google_formatted_address, pii_input_address, pii_lat, pii_long, pii_private_notes,
pii_street_num"""

COLS_IN_ORDER = """id, parent_ticket_id, child_ticket, dept, status_name, status_code, request_type_name, 
request_type_id, origin, pii_comments, pii_private_notes, create_date_est, create_date_utc, create_date_unix, 
last_action_est, last_action_unix, last_action_utc, closed_date_est, closed_date_utc, closed_date_unix, 
pii_street_num, street, cross_street, street_id, cross_street_id, city, pii_input_address, 
pii_google_formatted_address, address_type, anon_google_formatted_address, neighborhood_name, 
council_district, ward, police_zone, fire_zone, dpw_streets, dpw_enviro, dpw_parks, pii_lat, pii_long, anon_lat, 
anon_long"""

SPLIT_COLS_IN_ORDER = """id, dept, status_name, status_code, 
request_type_name, request_type_id, pii_comments, pii_private_notes, create_date_est, create_date_utc, 
create_date_unix, last_action_est, last_action_unix, last_action_utc, closed_date_est, closed_date_utc, closed_date_unix, 
pii_street_num, street, cross_street, street_id, cross_street_id, city, pii_input_address, 
pii_google_formatted_address, address_type, anon_google_formatted_address, neighborhood_name, 
council_district, ward, police_zone, fire_zone, dpw_streets, dpw_enviro, dpw_parks, pii_lat, pii_long, anon_lat, 
anon_long"""

LINKED_COLS_IN_ORDER = """id, num_requests, dept, status_name, status_code, 
request_type_name, request_type_id, pii_comments, pii_private_notes, create_date_est, create_date_utc, 
create_date_unix, last_action_est, last_action_unix, last_action_utc, closed_date_est, closed_date_utc, closed_date_unix, 
pii_street_num, street, cross_street, street_id, cross_street_id, city, pii_input_address, 
pii_google_formatted_address, address_type, anon_google_formatted_address, neighborhood_name, 
council_district, ward, police_zone, fire_zone, dpw_streets, dpw_enviro, dpw_parks, pii_lat, pii_long, anon_lat, 
anon_long"""

SAFE_COLS_IN_ORDER = """id, num_requests, dept, status_name, status_code, 
request_type_name, request_type_id, create_date_est, create_date_utc, create_date_unix, last_action_est, 
last_action_unix, last_action_utc, closed_date_est, closed_date_unix, closed_date_utc, street, cross_street, street_id, 
cross_street_id, city, address_type, anon_google_formatted_address, neighborhood_name, council_district, ward, 
police_zone, fire_zone, dpw_streets, dpw_enviro, dpw_parks, anon_lat, anon_long"""


# TODO: change interval to 5 min. Alter bucket/table declarations appropriately
# This DAG will run every 5 min which is a departure from our SOP. the schedule interval reflects this in CRON
# nomemclature. The 5 min interval was chosen to accomodate WPRDC's needs.
dag = DAG(
        'qalert_requests',
        default_args = default_args,
        schedule_interval = '@daily',
        user_defined_filters = {'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year}
)


# Run gcs_loader
qalert_requests_gcs = BashOperator(
        task_id = 'qalert_gcs',
        bash_command = F"python {os.environ['DAGS_PATH']}/dependencies/gcs_loaders/qalert_gcs.py",
        dag = dag
)


# Run dataflow_script
df_cmd_str = format_dataflow_call("qalert_requests_dataflow.py")
qalert_requests_dataflow = BashOperator(
        task_id = 'qalert_dataflow',
        bash_command = df_cmd_str,
        dag = dag
)


# Load AVRO data produced by dataflow_script into BQ temp table
qalert_requests_bq = GoogleCloudStorageToBigQueryOperator(
        task_id = 'qalert_bq',
        destination_project_dataset_table = f"{os.environ['GCLOUD_PROJECT']}:qalert.new_req",
        bucket = f"{os.environ['GCS_PREFIX']}_qalert",
        source_objects = ["requests/avro_output/{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ ds }}/*.avro"],
        write_disposition = 'WRITE_TRUNCATE',
        create_disposition = 'CREATE_IF_NEEDED',
        source_format = 'AVRO',
        autodetect = True,
        dag = dag
)


# Update geo coords with lat/long cast as floats (dataflow/AVRO glitch forces them to be output as strings; the
# source of the error is instrinsic to dataflow and may not be fixable). Also, dedupe the results (someties the same
# ticket appears in the computer system more than 1 time (a QAlert glitch)
query_format = f"""
WITH formatted AS
    (
    SELECT 
        DISTINCT * EXCEPT (pii_lat, pii_long, anon_lat, anon_long),
        CAST(pii_lat AS FLOAT64) AS pii_lat,
        CAST(pii_long AS FLOAT64) AS pii_long,
        CAST(anon_lat AS FLOAT64) AS anon_lat,
        CAST(anon_long AS FLOAT64) AS anon_long,
    FROM 
        {os.environ['GCLOUD_PROJECT']}.qalert.new_req
    )
SELECT {COLS_IN_ORDER} FROM formatted
"""
qalert_requests_format_dedupe = BigQueryOperator(
        task_id = 'qalert_dedupe_and_format',
        sql = query_format,
        use_legacy_sql = False,
        destination_dataset_table = f"{os.environ['GCLOUD_PROJECT']}:qalert.new_req",
        write_disposition = 'WRITE_TRUNCATE',
        dag = dag
)


# Query new tickets to determine if they are in the city limits
query_city_lim = build_city_limits_query('qalert', 'new_req', 'pii_lat', 'pii_long')
qalert_requests_city_limits = BigQueryOperator(
        task_id = 'qalert_city_limits',
        sql = query_city_lim,
        use_legacy_sql = False,
        dag = dag
)


# TODO: investigate (and if necessary fix) the unknown source of duplicates in the geojoin query (see util function
#  for clearer explanation)
# Join all the geo information (e.g. DPW districts, etc) to the new data
query_geo_join = build_revgeo_time_bound_query('qalert', 'new_req', 'create_date_est', 'id', 'pii_lat',
                                               'pii_long', COLS_IN_ORDER)
qalert_requests_geojoin = BigQueryOperator(
        task_id = 'qalert_geojoin',
        sql = query_geo_join,
        use_legacy_sql = False,
        dag = dag
)


# Append the geojoined and de-duped new_req to all_requests (replace table after append to order by ID. BQ does
# not allow this in INSERT statements (2021-10-01)
query_append = f"""
INSERT INTO {os.environ['GCLOUD_PROJECT']}.qalert.all_actions
SELECT 
    {COLS_IN_ORDER} 
FROM 
    `{os.environ['GCLOUD_PROJECT']}.qalert.new_geo_enriched_deduped`;
    
CREATE OR REPLACE TABLE {os.environ['GCLOUD_PROJECT']}.qalert.all_requests AS
SELECT 
    {COLS_IN_ORDER} 
FROM 
    {os.environ['GCLOUD_PROJECT']}.qalert.all_actions
ORDER BY id DESC
"""
qalert_requests_merge_new_tickets = BigQueryOperator(
        task_id = 'qalert_merge_new_tickets',
        sql = query_append,
        use_legacy_sql = False,
        dag = dag
)


# Split new tickets by parent/child status
query_split_parent = f"""
CREATE OR REPLACE TABLE `{os.environ['GCLOUD_PROJECT']}.qalert`.new_parents AS
SELECT 
    {SPLIT_COLS_IN_ORDER} 
FROM 
    `{os.environ['GCLOUD_PROJECT']}.qalert`.new_geo_enriched_deduped
WHERE child_ticket = False;

ALTER TABLE {os.environ['GCLOUD_PROJECT']}.qalert.new_parents
ADD COLUMN num_requests integer
"""
qalert_requests_split_new_parents = BigQueryOperator(
        task_id = 'qalert_requests_split_new_parents',
        sql = query_split_parent,
        use_legacy_sql = False,
        dag = dag
)


query_split_child = f"""
CREATE OR REPLACE TABLE {os.environ['GCLOUD_PROJECT']}.qalert.new_children AS
SELECT 
    {COLS_IN_ORDER} 
FROM 
    {os.environ['GCLOUD_PROJECT']}.qalert.new_geo_enriched_deduped
WHERE child_ticket = True
"""
qalert_requests_split_new_children = BigQueryOperator(
        task_id = 'qalert_requests_split_new_children',
        sql = query_split_child,
        use_legacy_sql = False,
        dag = dag
)


# Add new parents in all_linked_requests
query_append_new_parents = f"""
INSERT INTO {os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests
SELECT 
    {LINKED_COLS_IN_ORDER}
FROM 
    {os.environ['GCLOUD_PROJECT']}.qalert.new_parents;
"""
qalert_requests_append_new_parent_tickets = BigQueryOperator(
        task_id = 'qalert_requests_append_new_parent_tickets',
        sql = query_append_new_parents,
        use_legacy_sql = False,
        dag = dag
)



#TODO: STILL NEED TO UPDATE THE CLOSED STATUS, LAST ACTION, etc.
# Solution?: get the status/last action of the YOUNGEST child in the inner with block
# Select * except (above fields ^) from alr in the outer with block Instead, select the above fields (^) from the
# inner WITH block

# Update all linked tickets with the information from new children
query_update_parent = f"""
-- Create/Replace table after all nested WITH operators
CREATE OR REPLACE TABLE {os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests AS

    
    -- Outer WITH operator (all_appended): 
    WITH all_appended AS
    (
            
        -- inner with operator (child_combined): returns an id and total number of child requests from the 
        -- new_children table that correspond to a parent ticket.  
        -- (the count of child tickets in each batch will always be >= 1 for each ticket, and has no upper bound)
        -- also concatenates all the pii_comments and pii_private_notes for each group of children together
        WITH child_combined AS 
        
        (
        SELECT
            nc.parent_ticket_id AS match_id,
            (COUNT(nc.parent_ticket_id)) AS n_new_child_requests,
            STRING_AGG(CONCAT(nc.pii_comments)) AS child_pii_comments,
            STRING_AGG(CONCAT(nc.pii_private_notes)) AS child_pii_notes,
        FROM
            {os.environ['GCLOUD_PROJECT']}.qalert.new_children nc
        GROUP BY match_id
        )
    
        -- Outer WITH operator (all_appended):
        -- these results represent all of the information in all_linked_requests 
        -- prior to the append job, that was then updated with total request counts and comment/note concatentation         
        -- Thus, operating on the results derived from the inner WITH operator (child_combined) & the (already existing, 
        -- and soon to be overwritten) all_linked_requests table return:
        --         1) all cols of the linked tickets (all_linked_requests) 
        --         2) the sum of new child tickets belonging to a parent and the parent itself (1 OR MORE children for 
        --              some parents in each batch of new requests) (e.g. 1 parent and 2 children: num_requests = 3)
        --         3) concat the pii_comments and pii_private_notes of the children and parents 
        SELECT
            alr.id, 
            IFNULL(n_new_child_requests, 0) + IFNULL(alr.num_requests, 1) AS num_requests,
            CONCAT(pii_comments, child_pii_comments) AS pii_comments,
            CONCAT(pii_private_notes, child_pii_notes) AS pii_private_notes,
            alr.* EXCEPT (id, pii_comments, pii_private_notes, num_requests)
        FROM
            child_combined
        RIGHT OUTER JOIN
            {os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests alr ON 
            match_id = alr.id
    )


-- operating on all results from the outer WITH operator (all_appended): 
-- overwrite the existing table all_linked_requests with the newly updated results of the above queries 
SELECT {LINKED_COLS_IN_ORDER} FROM all_appended
"""
qalert_requests_update_parent_tickets = BigQueryOperator(
        task_id = 'qalert_requests_update_parent_tickets',
        sql = query_update_parent,
        use_legacy_sql = False,
        dag = dag
)


# Create a table from all_linked_requests that has all columns EXCEPT those that have potential PII. This table is
# subsequently exported to WPRDC. BQ will not currently (2021-10-01) allow data to be pushed from a query and it must
# be stored in a table prior to the push. Thus, this is a 2 step process also involving the operator below.
query_drop_pii = f"""
CREATE OR REPLACE TABLE `{os.environ['GCLOUD_PROJECT']}.qalert.data_export_scrubbed` AS
SELECT 
    {SAFE_COLS_IN_ORDER}
FROM 
    `{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests`
"""
qalert_requests_drop_pii_for_export = BigQueryOperator(
        task_id = 'qalert_requests_drop_pii_for_export',
        sql = query_drop_pii,
        use_legacy_sql = False,
        dag = dag
)


# Export table as CSV to WPRDC bucket
qalert_wprdc_export = BigQueryToCloudStorageOperator(
        task_id = 'qalert_wprdc_export',
        source_project_dataset_table = f"{os.environ['GCLOUD_PROJECT']}:qalert.data_export_scrubbed",
        destination_cloud_storage_uris = [f"gs://{os.environ['GCS_PREFIX']}_wprdc/qalert_requests_" + "{{ ds }}.csv"],
        dag = dag
)


# TODO: Data Services team has IAM issues preventing access to Web Team BQ. Uncomment when this is resolved.
# Export to Web Dev team BQ table
# query_push_to_web_team = f"""
# CREATE OR REPLACE TABLE `{os.environ["GCLOUD_WEB_DEV_PROJECT"]}.qalert.data_export_scrubbed` AS
# SELECT
#   {SAFE_COLS_IN_ORDER}
# FROM
#   `{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests`
# """
# qalert_requests_push_to_web_team= BigQueryOperator(
#         task_id = 'qalert_requests_push_to_web_teeam',
#         sql = query_push_to_web_team,
#         use_legacy_sql = False,
#         dag = dag
# )


#TODO: this code block was added as part of the dubugging process and it is unclear if it is needed. Will keep/remove
# (remember to remove the import statement also)
# when that is more clear.
delete_new_req = BigQueryTableDeleteOperator(
    task_id="delete_new_req",
    deletion_dataset_table=f"{os.environ['GCLOUD_PROJECT']}.qalert.new_req",
    dag = dag
)


# Clean up
qalert_beam_cleanup = BashOperator(
        task_id = 'qalert_beam_cleanup',
        bash_command = airflow_utils.beam_cleanup_statement(f"{os.environ['GCS_PREFIX']}_qalert"),
        dag = dag
)



# DAG execution:
qalert_requests_gcs >> qalert_requests_dataflow >> qalert_requests_bq >> qalert_requests_format_dedupe >> \
qalert_requests_city_limits >> qalert_requests_geojoin >> qalert_requests_merge_new_tickets >> \
qalert_requests_split_new_parents >> qalert_requests_split_new_children >> \
qalert_requests_append_new_parent_tickets >> qalert_requests_update_parent_tickets >> \
qalert_requests_drop_pii_for_export >> qalert_wprdc_export >> delete_new_req >> qalert_beam_cleanup

# TODO: Insert as the final operation before delete_new_req when IAM issues for the web dev project are resolved
# qalert_requests_push_to_web_team >
