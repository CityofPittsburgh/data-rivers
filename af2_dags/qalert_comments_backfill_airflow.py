# TODO: When Airflow 2.0 is released, upgrade the package, sub in DataFlowPythonOperator for BashOperator,
#  and pass the argument 'py_interpreter=python3'


from __future__ import absolute_import

import os

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator

from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_year, get_ds_month, get_ds_day, default_args, build_split_table_query
from dependencies.bq_queries import general_queries, geo_queries

COLS_IN_ORDER = """id, parent_ticket_id, child_ticket, dept, status_name, status_code, request_type_name, 
request_type_id, origin, pii_comments, anon_comments, pii_private_notes, create_date_est, create_date_utc, 
create_date_unix, last_action_est, last_action_utc, last_action_unix, closed_date_est, closed_date_utc, 
closed_date_unix, pii_street_num, street, cross_street, street_id, cross_street_id, city, pii_input_address, 
address_type, neighborhood_name, council_district, ward, police_zone, fire_zone, dpw_streets, dpw_enviro, 
dpw_parks, pii_lat, pii_long, anon_lat, anon_long"""

LINKED_COLS_IN_ORDER = """status_name, status_code, dept, 
request_type_name, request_type_id, origin, pii_comments, anon_comments, pii_private_notes, create_date_est, 
create_date_utc, create_date_unix, last_action_est, last_action_utc, last_action_unix, closed_date_est, closed_date_utc, 
closed_date_unix, pii_street_num, street, cross_street, street_id, cross_street_id, city, pii_input_address, 
NULL AS pii_google_formatted_address, NULL AS anon_google_formatted_address, address_type, neighborhood_name, 
council_district, ward, police_zone, fire_zone, dpw_streets, dpw_enviro, dpw_parks,  
NULL AS google_pii_lat, NULL AS google_pii_long, NULL AS google_anon_lat, NULL AS google_anon_long, 
pii_lat AS input_pii_lat, pii_long AS input_pii_long, anon_lat AS input_anon_lat, anon_long AS input_anon_long"""

EXCLUDE_TYPES = """'Hold - 311', 'Graffiti, Owner Refused DPW Removal', 'Medical Exemption - Tote', 
'Snow Angel Volunteer', 'Claim form (Law)','Snow Angel Intake', 'Application Request', 'Reject to 311', 'Referral', 
'Question'"""

SAFE_FIELDS = """status_name, status_code, dept, 
request_type_name, request_type_id, origin, create_date_est, create_date_utc, 
create_date_unix, last_action_est, last_action_unix, last_action_utc, closed_date_est, closed_date_utc,  
closed_date_unix, street, cross_street, street_id, cross_street_id, city, anon_google_formatted_address, 
address_type, neighborhood_name, council_district, ward, police_zone, fire_zone, dpw_streets, 
dpw_enviro, dpw_parks, input_anon_lat, input_anon_long, google_anon_lat, google_anon_long"""

dag = DAG(
    'qalert_comments_backfill',
    default_args=default_args,
    schedule_interval=None,
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year,
                          'get_ds_day': get_ds_day}
)

path = "{{ ds|get_ds_year }}-{{ ds|get_ds_month }}-{{ ds|get_ds_day }}"

# Run gcs_loader
gcs_loader = BashOperator(
    task_id='gcs_loader',
    bash_command=F"python {os.environ['DAGS_PATH']}/dependencies/gcs_loaders/qalert_comments_backfill_gcs.py",
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
    destination_project_dataset_table=f"{os.environ['GCLOUD_PROJECT']}:qalert.incoming_backfill",
    bucket=f"{os.environ['GCS_PREFIX']}_qalert",
    source_objects=[f"requests/backfill/{path}/avro_output/*.avro"],
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    source_format='AVRO',
    autodetect=True,
    bigquery_conn_id='google_cloud_default',
    dag=dag
)

query_split_table = build_split_table_query('qalert', 'incoming_backfill', 1429529820, 1665686739,
                                            30, 'create_date_unix', COLS_IN_ORDER)
split_table = BigQueryOperator(
    task_id='split_table',
    sql=query_split_table,
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

# Query new tickets to determine if they are in the city limits
query_city_lim = ""
for i in range(30):
    query_city_lim += geo_queries.build_city_limits_query(f'incoming_backfill_{i + 1}', 'pii_lat', 'pii_long') + ";"
city_limits = BigQueryOperator(
    task_id='city_limits',
    sql=query_city_lim,
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

query_join_dedupe = f"""
CREATE OR REPLACE TABLE `{os.environ['GCLOUD_PROJECT']}.qalert.incoming_backfill` AS
SELECT * FROM `{os.environ['GCLOUD_PROJECT']}.qalert.incoming_backfill_1` 
"""
for i in range(1, 30):
    query_join_dedupe += F"""
    UNION DISTINCT 
    SELECT * FROM `{os.environ['GCLOUD_PROJECT']}.qalert.incoming_backfill_{i + 1}`
    """
join_dedupe = BigQueryOperator(
    task_id='join_dedupe',
    sql=query_join_dedupe,
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

# TODO: investigate (and if necessary fix) the unknown source of duplicates in the geojoin query (see util function
#  for clearer explanation)
# FINAL ENRICHMENT OF NEW DATA
# Join all the geo information (e.g. DPW districts, etc) to the new data
query_geo_join = geo_queries.build_revgeo_time_bound_query(
        dataset = 'qalert',
        source = F"`{os.environ['GCLOUD_PROJECT']}.qalert.incoming_backfill`",
        create_date = 'create_date_utc',lat_field = 'pii_lat', long_field = 'pii_long',
        new_table = F"`{os.environ['GCLOUD_PROJECT']}.qalert.backfill_enriched`")

geojoin = BigQueryOperator(
    task_id='geojoin',
    sql=query_geo_join,
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

dedup_enriched = BigQueryOperator(
    task_id='dedup_enriched',
    sql=general_queries.build_dedup_old_updates('qalert', 'backfill_enriched', 'id', 'last_action_unix'),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

query_add_pii_comments = f"""
CREATE OR REPLACE TABLE `{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests` AS
SELECT 
    alr.group_id, alr.child_ids, alr.num_requests, alr.parent_closed, alr.status_name, 
    alr.status_code, alr.dept, alr.request_type_name, alr.request_type_id, alr.origin,
    bck.pii_comments, alr.anon_comments, alr.pii_private_notes, alr.create_date_est, 
    alr.create_date_utc, alr.create_date_unix, alr.last_action_est, alr.last_action_utc, 
    alr.last_action_unix, alr.closed_date_est, alr.closed_date_utc, alr.closed_date_unix, 
    alr.pii_street_num, alr.street, alr.cross_street, alr.street_id, alr.cross_street_id, 
    alr.city, alr.pii_input_address, alr.pii_google_formatted_address, 
    alr.anon_google_formatted_address, alr.address_type, alr.neighborhood_name, 
    alr.council_district, alr.ward, alr.police_zone, alr.fire_zone, alr.dpw_streets, 
    alr.dpw_enviro, alr.dpw_parks, alr.google_pii_lat, alr.google_pii_long, 
    alr.google_anon_lat, alr.google_anon_long, alr.input_pii_lat, alr.input_pii_long,
    alr.input_anon_lat, alr.input_anon_long
FROM `{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests` alr
JOIN `{os.environ['GCLOUD_PROJECT']}.qalert.backfill_enriched` bck
ON alr.group_id = bck.id;

CREATE OR REPLACE TABLE `{os.environ['GCLOUD_PROJECT']}.qalert.all_tickets_current_status` AS
SELECT 
    atcs.id, atcs.parent_ticket_id, atcs.child_ticket, atcs.dept, atcs.status_name, 
    atcs.status_code, atcs.request_type_name, atcs.request_type_id, atcs.origin,
    bck.pii_comments, atcs.anon_comments, atcs.pii_private_notes, atcs.create_date_est, 
    atcs.create_date_utc, atcs.create_date_unix, atcs.last_action_est, atcs.last_action_utc, 
    atcs.last_action_unix, atcs.closed_date_est, atcs.closed_date_utc, atcs.closed_date_unix, 
    atcs.pii_street_num, atcs.street, atcs.cross_street, atcs.street_id, atcs.cross_street_id, 
    atcs.city, atcs.pii_input_address, atcs.pii_google_formatted_address, 
    atcs.anon_google_formatted_address, atcs.address_type, atcs.neighborhood_name, 
    atcs.council_district, atcs.ward, atcs.police_zone, atcs.fire_zone, atcs.dpw_streets, 
    atcs.dpw_enviro, atcs.dpw_parks, atcs.google_pii_lat, atcs.google_pii_long, 
    atcs.google_anon_lat, atcs.google_anon_long, atcs.input_pii_lat, atcs.input_pii_long,
    atcs.input_anon_lat, atcs.input_anon_long
FROM `{os.environ['GCLOUD_PROJECT']}.qalert.all_tickets_current_status` atcs
JOIN `{os.environ['GCLOUD_PROJECT']}.qalert.backfill_enriched` bck
ON atcs.id = bck.id;
"""
add_pii_comments = BigQueryOperator(
    task_id='add_pii_comments',
    sql=query_add_pii_comments,
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

query_insert_new_parent = f"""
/*
This query check that a ticket has never been seen before (checks all_tix_current_status) AND
that the ticket is a parent. Satisfying both conditions means that the ticket needs to be placed in all_linked_requests
There is one catch that is caused by the way tickets are manually linked: This newly recorded request is
labeled as a parent. However, in the future the 311 operators may  linke this ticket with another
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
    `{os.environ['GCLOUD_PROJECT']}.qalert.backfill_enriched`
WHERE id NOT IN (SELECT id FROM `{os.environ['GCLOUD_PROJECT']}.qalert.all_tickets_current_status`)
AND child_ticket = False
AND request_type_name NOT IN ({EXCLUDE_TYPES})
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
FROM `{os.environ['GCLOUD_PROJECT']}.qalert.backfill_enriched`
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

query_integrate_children = f"""
/*
In query_remove_false_parents: false parent tix were found and eliminated.

This query will combine together all the relevant information for each child ticket (within its linkage family).

One complication of the workflow is the false parent tickets that were identified and eliminated in
query_remove_false_parents.
The incoming information from the child ticket (not the information from the false_parent entry into
all_linked_requests) was extracted in that query. Identification and deletion of false parents, and extraction of
the corresponding child's information, occurs at the earliest instance that the false parent is discovered. Thus,
the information from the child ticket's processed in query_remove_false_parents can be considered a child that has
never been observed before. This ultimately means that the newly observed child's data needs to be combined into
the other newly identified children (those which were never associated with a false parent).
Thus, the need to combine ALL OF THE CHILD TICKETS (both those associated with a false parent and those never being
misrepresented) is handled by this query
*/
CREATE OR REPLACE TABLE `{os.environ['GCLOUD_PROJECT']}.qalert.temp_child_combined` AS
(
    -- children never seen before and without a false parent
    WITH new_children AS
    (
    SELECT
        id, parent_ticket_id, anon_comments, pii_private_notes
    FROM
        `{os.environ['GCLOUD_PROJECT']}.qalert.backfill_enriched` new_c
    WHERE new_c.id NOT IN (SELECT id FROM `{os.environ['GCLOUD_PROJECT']}.qalert.all_tickets_current_status`)
    AND new_c.child_ticket = TRUE
    AND new_c.request_type_name NOT IN ({EXCLUDE_TYPES})
    ),

    -- children above plus the children of false parent tickets
    combined_children AS
    (
    SELECT *
    FROM new_children

    UNION ALL

    SELECT fp_id AS id, parent_ticket_id, anon_comments, pii_private_notes
    FROM `{os.environ['GCLOUD_PROJECT']}.qalert.temp_prev_parent_now_child`
    ),

    -- from ALL children tickets, concatenate the necessary string data
    concat_fields AS
    (
    SELECT
        parent_ticket_id AS concat_p_id,
        STRING_AGG(id, ", ") AS child_ids,
        STRING_AGG(anon_comments, " <BREAK> ") AS child_anon_comments,
        STRING_AGG(pii_private_notes, " <BREAK> ") AS child_pii_notes
    FROM combined_children
    GROUP BY concat_p_id
    ),

    -- Sum all children within the linkage family
    child_count AS
    (
        SELECT
            parent_ticket_id AS p_id,
            COUNT(id) AS cts
        FROM combined_children
        GROUP BY p_id
    )

    -- Selection of all above processing into a temp table
    SELECT
        child_count.*,
        concat_fields.* EXCEPT (concat_p_id)
    FROM child_count
    JOIN concat_fields ON
    child_count.p_id = concat_fields.concat_p_id
);

-- update existing entries inside all_linked_requests
UPDATE `{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests` alr
SET alr.num_requests = tcc.cts + alr.num_requests
FROM `{os.environ['GCLOUD_PROJECT']}.qalert.temp_child_combined` tcc
WHERE alr.group_id = tcc.p_id;

UPDATE `{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests` alr
SET alr.child_ids =  CONCAT(alr.child_ids,tcc.child_ids)
FROM `{os.environ['GCLOUD_PROJECT']}.qalert.temp_child_combined` tcc
WHERE alr.group_id = tcc.p_id;

UPDATE `{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests` alr
SET alr.anon_comments =  CONCAT(alr.anon_comments,tcc.child_anon_comments)
FROM `{os.environ['GCLOUD_PROJECT']}.qalert.temp_child_combined` tcc
WHERE alr.group_id = tcc.p_id;

UPDATE `{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests` alr
SET alr.pii_private_notes =  CONCAT(alr.pii_private_notes,tcc.child_pii_notes)
FROM `{os.environ['GCLOUD_PROJECT']}.qalert.temp_child_combined` tcc
WHERE alr.group_id = tcc.p_id;
"""
integrate_children = BigQueryOperator(
    task_id='integrate_children',
    sql=query_integrate_children,
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

# Create a table from all_linked_requests that has all columns EXCEPT those that have potential PII. This table is
# subsequently exported to WPRDC. BQ will not currently (2021-10-01) allow data to be pushed from a query and it must
# be stored in a table prior to the push. Thus, this is a 2 step process also involving the operator below.
query_drop_pii = f"""
CREATE OR REPLACE TABLE `{os.environ['GCLOUD_PROJECT']}.qalert.data_export_scrubbed` AS
SELECT
    group_id,
    child_ids,
    num_requests,
    parent_closed,
    {SAFE_FIELDS}
FROM
    `{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests`
"""
drop_pii_for_export = BigQueryOperator(
    task_id='drop_pii_for_export',
    sql=query_drop_pii,
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
gcs_loader >> dataflow >> gcs_to_bq >> split_table >> city_limits >> join_dedupe >> geojoin >> dedup_enriched >> \
    add_pii_comments >> insert_new_parent >> remove_false_parents >> integrate_children >> drop_pii_for_export >> \
    wprdc_export >> beam_cleanup
