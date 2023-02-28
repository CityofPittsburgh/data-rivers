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
# 2.) Make note of total ticket count present in Qscend application and compare it to the results of this backfill

# This DAG schedule interval set to None because it will only ever be triggered manually
dag = DAG(
    'qalert_request_type_backfill',
    default_args=default_args,
    schedule_interval=None,
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year,
                          'get_ds_day': get_ds_day}
)

path = "{{ ds|get_ds_year }}-{{ ds|get_ds_month }}-{{ ds|get_ds_day }}"

# Run gcs_loader
gcs_loader = BashOperator(
    task_id='gcs_loader',
    bash_command=F"python {os.environ['DAGS_PATH']}/dependencies/gcs_loaders/qalert_request_type_backfill_gcs.py",
    dag=dag
)

# Run dataflow_script
py_cmd = f"python {os.environ['DAGS_PATH']}/dependencies/dataflow_scripts/qalert_request_type_backfill_dataflow.py"
in_cmd = \
    f" --input gs://{os.environ['GCS_PREFIX']}_qalert/requests/backfill/request_type_backfill/{path}/backfilled_req_type_requests.json"
out_cmd = f" --avro_output gs://{os.environ['GCS_PREFIX']}_qalert/requests/backfill/request_type_backfill/{path}/avro_output/"
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
    source_objects=[f"requests/backfill/request_type_backfill/{path}/avro_output/*.avro"],
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
query_sync_update = build_sync_update_query('qalert', 'temp_backfill', 'temp_backfill_subset', 'id', upd_fields)
update_address_types = BigQueryOperator(
    task_id='update_address_types',
    sql=query_sync_update,
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
    `{os.environ['GCLOUD_PROJECT']}.qalert.temp_backfill`
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
CREATE OR REPLACE TABLE `{os.environ['GCLOUD_PROJECT']}.qalert.backfill_temp_prev_parent_now_child` AS
SELECT 
    id AS fp_id, parent_ticket_id, anon_comments, pii_private_notes
FROM `{os.environ['GCLOUD_PROJECT']}.qalert.temp_backfill`
WHERE id IN (SELECT 
                group_id
             FROM`{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests`)
AND child_ticket = TRUE ;

-- delete the false parent ticket's information 
DELETE FROM `{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests`
WHERE group_id IN 
        (SELECT fp_id FROM `{os.environ['GCLOUD_PROJECT']}.qalert.backfill_temp_prev_parent_now_child`);
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
CREATE OR REPLACE TABLE `{os.environ['GCLOUD_PROJECT']}.qalert.backfill_temp_child_combined` AS
(
    -- children never seen before and without a false parent
    WITH new_children AS
    (
    SELECT
        id, parent_ticket_id, anon_comments, pii_private_notes
    FROM
        `{os.environ['GCLOUD_PROJECT']}.qalert.temp_backfill` new_c
    WHERE new_c.id NOT IN (SELECT id FROM `{os.environ['GCLOUD_PROJECT']}.qalert.all_tickets_current_status`)
    AND new_c.child_ticket = TRUE
    ),

    -- children above plus the children of false parent tickets
    combined_children AS
    (
    SELECT *
    FROM new_children

    UNION ALL

    SELECT fp_id AS id, parent_ticket_id, anon_comments, pii_private_notes
    FROM `{os.environ['GCLOUD_PROJECT']}.qalert.backfill_temp_prev_parent_now_child`
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
FROM `{os.environ['GCLOUD_PROJECT']}.qalert.backfill_temp_child_combined` tcc
WHERE alr.group_id = tcc.p_id;

UPDATE `{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests` alr
SET alr.child_ids =  CONCAT(alr.child_ids,tcc.child_ids)
FROM `{os.environ['GCLOUD_PROJECT']}.qalert.backfill_temp_child_combined` tcc
WHERE alr.group_id = tcc.p_id;

UPDATE `{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests` alr
SET alr.anon_comments =  CONCAT(alr.anon_comments,tcc.child_anon_comments)
FROM `{os.environ['GCLOUD_PROJECT']}.qalert.backfill_temp_child_combined` tcc
WHERE alr.group_id = tcc.p_id;

UPDATE `{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests` alr
SET alr.pii_private_notes =  CONCAT(alr.pii_private_notes,tcc.child_pii_notes)
FROM `{os.environ['GCLOUD_PROJECT']}.qalert.backfill_temp_child_combined` tcc
WHERE alr.group_id = tcc.p_id;
"""
integrate_children = BigQueryOperator(
    task_id='integrate_children',
    sql=query_integrate_children,
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

query_replace_last_update = f"""
/*
Tickets are continually updated throughout their life. In the vast majority of cases a ticket will be created and
then further processed (creating status changes) over a timeline which causes the ticket's lifespan to encompass
multiple
DAG runs. ONLY THE CURRENT STATUS of each ticket, which been updated since the last DAG run, is returned in the
current DAG run. Thus, a ticket appears multiple times in our DAG.

The only consequential information that consecutive updates contain are changes to the status, the time of the last
update of the status, and the closure time (if applicable). The fact that child and parent tickets refer to the
same underlying request creates the possibility that only a child OR parent could theoretically be updated. This
occurred prior to 08/21 and it is currently (01/22) unclear if this will continue to happen (the API was updated to
account for this). Most likely all relevant updates will by synchronized with the parent ticket. The most feasible
solution to extracting update status/times is to take this information ONLY from the parent ticket and disregard
changes to the child ticket. This query selects parent tickets which have been previously recorded in the system and
simply extracts and updates the status timestamp data from those tickets. This data is then updated in
all_linked_requests.
*/
CREATE OR REPLACE TABLE  `{os.environ['GCLOUD_PROJECT']}.qalert.backfill_temp_update` AS
(
SELECT
    id,
    IF (status_name = "closed", TRUE, FALSE) AS p_closed,
    closed_date_est, closed_date_utc,closed_date_unix,
    last_action_est, last_action_utc,last_action_unix,
    status_name, status_code, request_type_name, request_type_id,
    input_pii_lat, input_pii_long, input_anon_lat, input_anon_long,
    address_type, neighborhood_name, council_district, ward, police_zone, 
    fire_zone, dpw_streets, dpw_enviro, dpw_parks
FROM  `{os.environ['GCLOUD_PROJECT']}.qalert.temp_backfill`

WHERE id IN (SELECT id FROM `{os.environ['GCLOUD_PROJECT']}.qalert.all_tickets_current_status`)
AND child_ticket = FALSE
);

UPDATE `{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests` alr
SET alr.parent_closed = tu.p_closed
FROM `{os.environ['GCLOUD_PROJECT']}.qalert.backfill_temp_update` tu
WHERE alr.group_id = tu.id;

UPDATE `{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests` alr
SET alr.status_name = tu.status_name
FROM `{os.environ['GCLOUD_PROJECT']}.qalert.backfill_temp_update` tu
WHERE alr.group_id = tu.id;

UPDATE `{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests` alr
SET alr.status_code = tu.status_code
FROM `{os.environ['GCLOUD_PROJECT']}.qalert.backfill_temp_update` tu
WHERE alr.group_id = tu.id;

UPDATE `{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests` alr
SET alr.request_type_name = tu.request_type_name
FROM `{os.environ['GCLOUD_PROJECT']}.qalert.backfill_temp_update` tu
WHERE alr.group_id = tu.id
AND alr.request_type_name != tu.request_type_name;

UPDATE `{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests` alr
SET alr.request_type_id = tu.request_type_id
FROM `{os.environ['GCLOUD_PROJECT']}.qalert.backfill_temp_update` tu
WHERE alr.group_id = tu.id
AND alr.request_type_id != tu.request_type_id;

UPDATE `{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests` alr
SET alr.input_pii_lat = tu.input_pii_lat
FROM `{os.environ['GCLOUD_PROJECT']}.qalert.backfill_temp_update` tu
WHERE alr.group_id = tu.id
AND alr.input_pii_lat != tu.input_pii_lat;

UPDATE `{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests` alr
SET alr.input_pii_long = tu.input_pii_long
FROM `{os.environ['GCLOUD_PROJECT']}.qalert.backfill_temp_update` tu
WHERE alr.group_id = tu.id
AND alr.input_pii_long != tu.input_pii_long;

UPDATE `{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests` alr
SET alr.input_anon_lat = tu.input_anon_lat
FROM `{os.environ['GCLOUD_PROJECT']}.qalert.backfill_temp_update` tu
WHERE alr.group_id = tu.id
AND alr.input_anon_lat != tu.input_anon_lat;

UPDATE `{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests` alr
SET alr.input_anon_long = tu.input_anon_long
FROM `{os.environ['GCLOUD_PROJECT']}.qalert.backfill_temp_update` tu
WHERE alr.group_id = tu.id
AND alr.input_anon_long != tu.input_anon_long;

UPDATE `{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests` alr
SET alr.address_type = tu.address_type
FROM `{os.environ['GCLOUD_PROJECT']}.qalert.backfill_temp_update` tu
WHERE alr.group_id = tu.id
AND alr.address_type != tu.address_type;

UPDATE `{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests` alr
SET alr.neighborhood_name = tu.neighborhood_name
FROM `{os.environ['GCLOUD_PROJECT']}.qalert.backfill_temp_update` tu
WHERE alr.group_id = tu.id
AND alr.neighborhood_name != tu.neighborhood_name;

UPDATE `{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests` alr
SET alr.council_district = tu.council_district
FROM `{os.environ['GCLOUD_PROJECT']}.qalert.backfill_temp_update` tu
WHERE alr.group_id = tu.id
AND alr.council_district != tu.council_district;

UPDATE `{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests` alr
SET alr.ward = tu.ward
FROM `{os.environ['GCLOUD_PROJECT']}.qalert.backfill_temp_update` tu
WHERE alr.group_id = tu.id
AND alr.ward != tu.ward;

UPDATE `{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests` alr
SET alr.police_zone = tu.police_zone
FROM `{os.environ['GCLOUD_PROJECT']}.qalert.backfill_temp_update` tu
WHERE alr.group_id = tu.id
AND alr.police_zone != tu.police_zone;

UPDATE `{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests` alr
SET alr.fire_zone = tu.fire_zone
FROM `{os.environ['GCLOUD_PROJECT']}.qalert.backfill_temp_update` tu
WHERE alr.group_id = tu.id
AND alr.fire_zone != tu.fire_zone;

UPDATE `{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests` alr
SET alr.dpw_streets = tu.dpw_streets
FROM `{os.environ['GCLOUD_PROJECT']}.qalert.backfill_temp_update` tu
WHERE alr.group_id = tu.id
AND alr.dpw_streets != tu.dpw_streets;

UPDATE `{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests` alr
SET alr.dpw_enviro = tu.dpw_enviro
FROM `{os.environ['GCLOUD_PROJECT']}.qalert.backfill_temp_update` tu
WHERE alr.group_id = tu.id
AND alr.dpw_enviro != tu.dpw_enviro;

UPDATE `{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests` alr
SET alr.dpw_parks = tu.dpw_parks
FROM `{os.environ['GCLOUD_PROJECT']}.qalert.backfill_temp_update` tu
WHERE alr.group_id = tu.id
AND alr.dpw_parks != tu.dpw_parks;

UPDATE `{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests` alr
SET alr.closed_date_est = tu.closed_date_est
FROM `{os.environ['GCLOUD_PROJECT']}.qalert.backfill_temp_update` tu
WHERE alr.group_id = tu.id;

UPDATE `{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests` alr
SET alr.closed_date_utc = tu.closed_date_utc
FROM `{os.environ['GCLOUD_PROJECT']}.qalert.backfill_temp_update` tu
WHERE alr.group_id = tu.id;

UPDATE `{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests` alr
SET alr.closed_date_unix = tu.closed_date_unix
FROM `{os.environ['GCLOUD_PROJECT']}.qalert.backfill_temp_update` tu
WHERE alr.group_id = tu.id;

UPDATE `{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests` alr
SET alr.last_action_est = tu.last_action_est
FROM `{os.environ['GCLOUD_PROJECT']}.qalert.backfill_temp_update` tu
WHERE alr.group_id = tu.id;

UPDATE `{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests` alr
SET alr.last_action_utc = tu.last_action_utc
FROM `{os.environ['GCLOUD_PROJECT']}.qalert.backfill_temp_update` tu
WHERE alr.group_id = tu.id;

UPDATE `{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests` alr
SET alr.last_action_unix = tu.last_action_unix
FROM `{os.environ['GCLOUD_PROJECT']}.qalert.backfill_temp_update` tu
WHERE alr.group_id = tu.id;
"""
replace_last_update = BigQueryOperator(
    task_id='replace_last_update',
    sql=query_replace_last_update,
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

query_delete_old_insert_new_records = f"""
/*
All tickets that ever receive an update (or are simply created) should be stored with their current status
for historical purposes. This query does just that. The data are simply inserted into all_tickets_current_status. If
the ticket has been seen before, it is already in all_tickets_current_status. Rather than simply add the newest ticket,
this query also deletes the prior entry for that ticket. The key to understanding this is: The API does not return the
FULL HISTORY of each ticket, but rather a snapshot of the ticket's current status. This means that if the status is
updated
multiple times between DAG runs, only the final status is recorded. While the FULL HISTORY has obvious value, this is
not available and it less confusing to simply store a current snapshot of the ticket's history.

all_tickets_current_status is currently (01/22) maintained for historical purposes.  This table has less value for
analysis as the linkages between tickets are not taken into account.
*/

DELETE FROM `{os.environ['GCLOUD_PROJECT']}.qalert.all_tickets_current_status`
WHERE id IN (SELECT id FROM `{os.environ['GCLOUD_PROJECT']}.qalert.temp_backfill`);
INSERT INTO `{os.environ['GCLOUD_PROJECT']}.qalert.all_tickets_current_status`
SELECT 
    {COLS_IN_ORDER}
FROM `{os.environ['GCLOUD_PROJECT']}.qalert.temp_backfill`;
"""
delete_old_insert_new_records = BigQueryOperator(
    task_id='delete_old_insert_new_records',
    sql=query_delete_old_insert_new_records,
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
    insert_new_parent >> remove_false_parents >> integrate_children >> replace_last_update >> \
    delete_old_insert_new_records >> beam_cleanup
