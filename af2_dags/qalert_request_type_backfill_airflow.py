from __future__ import absolute_import

import os

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_year, get_ds_month, get_ds_day, default_args
from dependencies.bq_queries import general_queries, geo_queries
from dependencies.bq_queries.qscend import integrate_new_requests as i_q

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
# 3.) Create request_type_backfill/successful_run_log subdirectories within backfill directory in qalert GCS bucket

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
py_cmd = f"python {os.environ['DAGS_PATH']}/dependencies/dataflow_scripts/qalert_requests_dataflow.py"
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

cast_fields = [{'field': 'input_pii_lat', 'type': 'FLOAT64'},
               {'field': 'input_pii_long', 'type': 'FLOAT64'},
               {'field': 'google_pii_lat', 'type': 'FLOAT64'},
               {'field': 'google_pii_long', 'type': 'FLOAT64'},
               {'field': 'input_anon_lat', 'type': 'FLOAT64'},
               {'field': 'input_anon_long', 'type': 'FLOAT64'},
               {'field': 'google_anon_lat', 'type': 'FLOAT64'},
               {'field': 'google_anon_long', 'type': 'FLOAT64'}]
format_table = BigQueryOperator(
    task_id='format_table',
    sql=general_queries.build_format_dedup_query('qalert', 'temp_backfill', 'temp_backfill', cast_fields, COLS_IN_ORDER),
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
city_limits = BigQueryOperator(
    task_id='city_limits',
    sql=geo_queries.build_city_limits_query('temp_backfill_subset', 'input_pii_lat', 'input_pii_long'),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

dedup_src_table = BigQueryOperator(
    task_id='dedup_src_table',
    sql=general_queries.build_dedup_old_updates('qalert', 'temp_backfill', 'id', 'last_action_unix'),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

dedup_sub_table = BigQueryOperator(
    task_id='dedup_sub_table',
    sql=general_queries.build_dedup_old_updates('qalert', 'temp_backfill_subset', 'id', 'last_action_unix'),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

upd_fields = ['address_type']
update_address_types = BigQueryOperator(
    task_id='update_address_types',
    sql=general_queries.build_sync_update_query('qalert', 'temp_backfill', 'temp_backfill_subset', 'id', upd_fields),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

geojoin = BigQueryOperator(
    task_id='geojoin',
    sql=geo_queries.build_revgeo_time_bound_query(
            dataset = 'qalert',
            source = F"`{os.environ['GCLOUD_PROJECT']}.qalert.temp_backfill`",
            create_date = 'create_date_utc', lat_field = 'input_pii_lat',long_field = 'input_pii_long',
            new_table = F"`{os.environ['GCLOUD_PROJECT']}.qalert.backfill_enriched`"
    ),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

insert_new_parent = BigQueryOperator(
    task_id='insert_new_parent',
    sql=i_q.insert_new_parent('backfill_enriched', LINKED_COLS_IN_ORDER),
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
FROM `{os.environ['GCLOUD_PROJECT']}.qalert.backfill_enriched`
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
        `{os.environ['GCLOUD_PROJECT']}.qalert.backfill_enriched` new_c
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
FROM  `{os.environ['GCLOUD_PROJECT']}.qalert.backfill_enriched`

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
WHERE alr.group_id = tu.id;

UPDATE `{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests` alr
SET alr.request_type_id = tu.request_type_id
FROM `{os.environ['GCLOUD_PROJECT']}.qalert.backfill_temp_update` tu
WHERE alr.group_id = tu.id;

UPDATE `{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests` alr
SET alr.input_pii_lat = tu.input_pii_lat
FROM `{os.environ['GCLOUD_PROJECT']}.qalert.backfill_temp_update` tu
WHERE alr.group_id = tu.id;

UPDATE `{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests` alr
SET alr.input_pii_long = tu.input_pii_long
FROM `{os.environ['GCLOUD_PROJECT']}.qalert.backfill_temp_update` tu
WHERE alr.group_id = tu.id;

UPDATE `{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests` alr
SET alr.input_anon_lat = tu.input_anon_lat
FROM `{os.environ['GCLOUD_PROJECT']}.qalert.backfill_temp_update` tu
WHERE alr.group_id = tu.id;

UPDATE `{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests` alr
SET alr.input_anon_long = tu.input_anon_long
FROM `{os.environ['GCLOUD_PROJECT']}.qalert.backfill_temp_update` tu
WHERE alr.group_id = tu.id;

UPDATE `{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests` alr
SET alr.address_type = tu.address_type
FROM `{os.environ['GCLOUD_PROJECT']}.qalert.backfill_temp_update` tu
WHERE alr.group_id = tu.id;

UPDATE `{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests` alr
SET alr.neighborhood_name = tu.neighborhood_name
FROM `{os.environ['GCLOUD_PROJECT']}.qalert.backfill_temp_update` tu
WHERE alr.group_id = tu.id;

UPDATE `{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests` alr
SET alr.council_district = tu.council_district
FROM `{os.environ['GCLOUD_PROJECT']}.qalert.backfill_temp_update` tu
WHERE alr.group_id = tu.id;

UPDATE `{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests` alr
SET alr.ward = tu.ward
FROM `{os.environ['GCLOUD_PROJECT']}.qalert.backfill_temp_update` tu
WHERE alr.group_id = tu.id;

UPDATE `{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests` alr
SET alr.police_zone = tu.police_zone
FROM `{os.environ['GCLOUD_PROJECT']}.qalert.backfill_temp_update` tu
WHERE alr.group_id = tu.id;

UPDATE `{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests` alr
SET alr.fire_zone = tu.fire_zone
FROM `{os.environ['GCLOUD_PROJECT']}.qalert.backfill_temp_update` tu
WHERE alr.group_id = tu.id;

UPDATE `{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests` alr
SET alr.dpw_streets = tu.dpw_streets
FROM `{os.environ['GCLOUD_PROJECT']}.qalert.backfill_temp_update` tu
WHERE alr.group_id = tu.id;

UPDATE `{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests` alr
SET alr.dpw_enviro = tu.dpw_enviro
FROM `{os.environ['GCLOUD_PROJECT']}.qalert.backfill_temp_update` tu
WHERE alr.group_id = tu.id;

UPDATE `{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests` alr
SET alr.dpw_parks = tu.dpw_parks
FROM `{os.environ['GCLOUD_PROJECT']}.qalert.backfill_temp_update` tu
WHERE alr.group_id = tu.id;

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

delete_old_insert_new_records = BigQueryOperator(
    task_id='delete_old_insert_new_records',
    sql=i_q.delete_old_insert_new(COLS_IN_ORDER, 'backfill_enriched'),
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
gcs_loader >> dataflow >> gcs_to_bq >> format_table >> create_subset >> city_limits >> dedup_src_table >> \
    dedup_sub_table >> update_address_types >> geojoin >> insert_new_parent >> remove_false_parents >> \
    integrate_children >> replace_last_update >> delete_old_insert_new_records >> beam_cleanup
