from __future__ import absolute_import

import os

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_year, get_ds_month, get_ds_day, default_args, build_city_limits_query, \
    build_revgeo_time_bound_query, build_insert_new_records_query

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

# STEPS TO TAKE BEFORE EXECUTING DAG:
# 1.) Make backups of all_linked_requests and all_tickets_current_status in case something goes wrong
# 2.) Make note of total ticket count present in Qscend application and compare it to the results of this backfill

# This DAG schedule interval set to None because it will only ever be triggered manually
dag = DAG(
    'qalert_monthly_backfill',
    default_args=default_args,
    schedule_interval=None,  # '@daily',
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

query_format_subset = f"""
CREATE OR REPLACE TABLE `{os.environ['GCLOUD_PROJECT']}.qalert.temp_backfill_subset` AS
WITH formatted  AS
    (
    SELECT
        DISTINCT * EXCEPT (pii_lat, pii_long, anon_lat, anon_long),
        CAST(pii_lat AS FLOAT64) AS input_pii_lat,
        CAST(pii_long AS FLOAT64) AS input_pii_long,
        CAST(pii_lat AS FLOAT64) AS google_pii_lat,
        CAST(pii_long AS FLOAT64) AS google_pii_long,
        CAST(anon_lat AS FLOAT64) AS input_anon_lat,
        CAST(anon_long AS FLOAT64) AS input_anon_long,
        CAST(anon_lat AS FLOAT64) AS google_anon_lat,
        CAST(anon_long AS FLOAT64) AS google_anon_long,
    FROM
        {os.environ['GCLOUD_PROJECT']}.qalert.temp_backfill
    )
-- drop the final column through slicing the string. final column is added in next query
SELECT
    {COLS_IN_ORDER}
FROM
    formatted
WHERE id NOT IN (SELECT id FROM `{os.environ['GCLOUD_PROJECT']}.qalert.all_tickets_current_status`)
"""
format_subset = BigQueryOperator(
    task_id='format_subset',
    sql=query_format_subset,
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


query_geo_join = build_revgeo_time_bound_query('qalert', 'temp_backfill_subset', 'backfill_enriched',
                                               'create_date_utc', 'input_pii_lat', 'input_pii_long')
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
);
"""
insert_new_parent = BigQueryOperator(
    task_id='insert_new_parent',
    sql=query_insert_new_parent,
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
WHERE alr.group_id = tcc.p_id AND
(
    (alr.child_ids NOT LIKE CONCAT('%, ', tcc.child_ids, '%')
    OR alr.child_ids NOT LIKE CONCAT('%', tcc.child_ids, ', %'))
    AND alr.child_ids != tcc.child_ids
);

UPDATE `{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests` alr
SET alr.child_ids = CONCAT(IFNULL(CONCAT(alr.child_ids, ', '), ''),tcc.child_ids)
FROM `{os.environ['GCLOUD_PROJECT']}.qalert.backfill_temp_child_combined` tcc
WHERE alr.group_id = tcc.p_id AND
(
    (alr.child_ids NOT LIKE CONCAT('%, ', tcc.child_ids, '%')
    OR alr.child_ids NOT LIKE CONCAT('%', tcc.child_ids, ', %'))
    AND alr.child_ids != tcc.child_ids
);

UPDATE `{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests` alr
SET alr.anon_comments = CONCAT(IFNULL(CONCAT(alr.anon_comments, ' <BREAK> '), ''),tcc.child_anon_comments)
FROM `{os.environ['GCLOUD_PROJECT']}.qalert.backfill_temp_child_combined` tcc
WHERE alr.group_id = tcc.p_id AND
(
    (alr.child_ids NOT LIKE CONCAT('%, ', tcc.child_ids, '%')
    OR alr.child_ids NOT LIKE CONCAT('%', tcc.child_ids, ', %'))
    AND alr.child_ids != tcc.child_ids
);

UPDATE `{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests` alr
SET alr.pii_private_notes = CONCAT(IFNULL(CONCAT(alr.pii_private_notes, ' <BREAK> '), ''),tcc.child_pii_notes)
FROM `{os.environ['GCLOUD_PROJECT']}.qalert.backfill_temp_child_combined` tcc
WHERE alr.group_id = tcc.p_id AND
(
    (alr.child_ids NOT LIKE CONCAT('%, ', tcc.child_ids, '%')
    OR alr.child_ids NOT LIKE CONCAT('%', tcc.child_ids, ', %'))
    AND alr.child_ids != tcc.child_ids
);
"""
integrate_children = BigQueryOperator(
    task_id='integrate_children',
    sql=query_integrate_children,
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
    integrate_children >> insert_missed_requests >> beam_cleanup
