from __future__ import absolute_import

import os

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator

from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_year, get_ds_month, default_args, \
    format_gcs_call, format_dataflow_call, build_city_limits_query, build_revgeo_time_bound_query

# TODO: When Airflow 2.0 is released, upgrade the package, sub in DataFlowPythonOperator for BashOperator,
#  and pass the argument 'py_interpreter=python3'


PII_FIELDS = """pii_comments, pii_google_formatted_address, pii_input_address, pii_lat, pii_long, pii_private_notes,
pii_street_num"""

COLS_IN_ORDER = """id, parent_ticket_id, child_ticket, dept, status, status_code, request_type_name, 
request_type_id, origin, pii_comments, pii_private_notes, create_date_est, create_date_utc, create_date_unix, 
last_action_est, last_action_unix, last_action_utc, closed_date_est, closed_date_unix, closed_date_utc,  
pii_street_num, street, cross_street, street_id, cross_street_id, city, pii_input_address, 
pii_google_formatted_address, address_type, anon_google_formatted_address, boundary_date_id, neighborhood_name, 
council_district, ward, police_zone, fire_zone, dpw_streets, dpw_enviro, dpw_parks, pii_lat, pii_long, anon_lat, 
anon_long"""

# This DAG will run every 5 min which is a departure from our SOP. the schedule interval reflects this in CRON
# nomemclature. The 5 min interval was chosen to accomodate WPRDC's needs.

# TODO: change interval to 5 min. Alter bucket/table declarations appropriately
dag = DAG(
        'qalert_requests',
        default_args = default_args,
        schedule_interval = '@daily',
        user_defined_filters = {'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year}
)

# Run gcs_loader
gcs_cmd_str = format_gcs_call("qalert_gcs.py", f"{os.environ['GCS_PREFIX']}_qalert", "requests")
qalert_requests_gcs = BashOperator(
        task_id = 'qalert_gcs',
        bash_command = gcs_cmd_str,
        dag = dag
)

# Run dataflow_script
qalert_requests_dataflow = BashOperator(
        task_id = 'qalert_dataflow',
        bash_command = format_dataflow_call("qalert_requests_dataflow.py"),
        dag = dag
)

# Load AVRO data produced by dataflow_script into BQ temp table
qalert_requests_bq = GoogleCloudStorageToBigQueryOperator(
        task_id = 'qalert_bq',
        destination_project_dataset_table = f"{os.environ['GCLOUD_PROJECT']}:qalert.temp_new_req",
        bucket = f"{os.environ['GCS_PREFIX']}_qalert",
        source_objects = ["requests/avro_output/{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ ds }}/*.avro"],
        write_disposition = 'WRITE_TRUNCATE',
        create_disposition = 'CREATE_IF_NEEDED',
        source_format = 'AVRO',
        autodetect = True,
        time_partitioning = {'type': 'DAY'},
        dag = dag
)

# Update geo coords with lat/long cast as floats (dataflow/AVRO glitch forces them to be output as strings; the
# source of the error is instrinsic to dataflow and may not be fixable)
format_query = f"""
WITH formatted AS
(SELECT DISTINCT * EXCEPT (pii_lat, pii_long, anon_lat, anon_long),
    CAST(pii_lat AS FLOAT64) AS pii_lat,
    CAST(pii_long AS FLOAT64) AS pii_long,
    CAST(anon_lat AS FLOAT64) AS anon_lat,
    CAST(anon_long AS FLOAT64) AS anon_long,
FROM {os.environ['GCLOUD_PROJECT']}.qalert.temp_new_req)
SELECT {COLS_IN_ORDER} FROM formatted
"""
qalert_requests_format_dedupe = BigQueryOperator(
        task_id = 'qalert_dedupe_and_format',
        sql = format_query,
        use_legacy_sql = False,
        destination_dataset_table = f"{os.environ['GCLOUD_PROJECT']}:qalert.temp_new_req",
        write_disposition = 'WRITE_TRUNCATE',
        time_partitioning = {'type': 'DAY'},
        dag = dag
)

# Query new tickets to determine if they are in the city limits
city_lim_query = build_city_limits_query('qalert', 'temp_new_req', 'pii_lat', 'pii_long')
qalert_requests_city_limits = BigQueryOperator(
        task_id = 'qalert_city_limits',
        sql = city_lim_query,
        use_legacy_sql = False,
        dag = dag
)

# TODO: investigate (and if necessary fix) the unknown source of duplicates in the geojoin query (see util function
#  for clearer explanation)
# Join all the geo information (e.g. DPW districts, etc) to the new data
geo_join_query = build_revgeo_time_bound_query('qalert', 'temp_new_req', 'create_date_est', 'id', 'pii_lat',
                                               'pii_long', "COLS_IN_ORDER")
qalert_requests_geojoin = BigQueryOperator(
        task_id = 'qalert_geojoin',
        sql = geo_join_query,
        use_legacy_sql = False,
        dag = dag
)

# Append the geojoined and de-duped temp_new_req to all_requests
append_query = f"""
INSERT INTO {os.environ['GCLOUD_PROJECT']}.qalert.all_requests
SELECT {COLS_IN_ORDER} FROM `data-rivers-testing.qalert.new_geo_enriched_dedupe`
"""
qalert_requests_merge_new_tickets = BigQueryOperator(
        task_id = 'qalert_merge_new_tickets',
        sql = append_query,
        use_legacy_sql = False,
        dag = dag
)

# Split new tickets by parent/child status
split_parent_query = f"""
CREATE OR REPLACE VIEW `{os.environ['GCLOUD_PROJECT']}.qalert`.new_parents AS
(SELECT {COLS_IN_ORDER} FROM `{os.environ['GCLOUD_PROJECT']}.qalert`.temp_new_req
WHERE child_ticket = False)
"""
qalert_requests_split_new_parents = BigQueryOperator(
        task_id = 'qalert_requests_split_new_parents',
        sql = split_parent_query,
        use_legacy_sql = False,
        write_disposition = 'WRITE_TRUNCATE',
        dag = dag
)

split_child_query = f"""
CREATE OR REPLACE VIEW {os.environ['GCLOUD_PROJECT']}.qalert.new_children AS
SELECT {COLS_IN_ORDER} FROM {os.environ['GCLOUD_PROJECT']}.qalert.temp_new_req
WHERE child_ticket = True
"""
qalert_requests_split_new_children = BigQueryOperator(
        task_id = 'qalert_requests_split_new_children',
        sql = split_child_query,
        use_legacy_sql = False,
        write_disposition = 'WRITE_TRUNCATE',
        dag = dag
)

# Add new parents in all_linked_requests
append_new_parents_query = f"""
INSERT INTO {os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests
SELECT {COLS_IN_ORDER} FROM {os.environ['GCLOUD_PROJECT']}.qalert.new_parents
"""
qalert_requests_append_new_parent_tickets = BigQueryOperator(
        task_id = 'qalert_requests_append_new_parent_tickets',
        sql = append_new_parents_query,
        use_legacy_sql = False,
        write_disposition = 'WRITE_APPEND',
        dag = dag
)

# Update all linked tickets with the information from new children
update_parent_query = f"""
WITH concat AS
(
SELECT
  * EXCEPT (id,
    pii_comments,
    pii_private_notes,
    num_requests),
  t3.id,
  CONCAT(t3.pii_comments, \n t4.pii_comments) pii_comments,
  CONCAT(t3.pii_private_notes, \n t4.pii_private_notes) pii_private_notes,
  t4.num_requests
FROM
  {os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests t3
LEFT OUTER JOIN (
  SELECT
    t1.id,
    (COUNT(*)+1) AS num_requests,
    STRING_AGG(CONCAT(t2.pii_comments \n)) pii_comments,
    STRING_AGG(CONCAT(t2.pii_private_notes \n)) pii_private_notes
  FROM
    {os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests t1
  LEFT OUTER JOIN
    {os.environ['GCLOUD_PROJECT']}.qalert.new_children t2
  ON
    t2.parent_ticket_id = t1.id
  GROUP BY
    t1.id) t4
ON
  t3.id = t4.id
)
SELECT {COLS_IN_ORDER} FROM concat
"""
qalert_requests_update_parent_tickets = BigQueryOperator(
        task_id = 'qalert_requests_update_parent_tickets',
        sql = update_parent_query,
        use_legacy_sql = False,
        destination_dataset_table = f"{os.environ['GCLOUD_PROJECT']}:qalert.all_linked_requests",
        write_disposition = 'WRITE_APPEND',
        dag = dag
)

# Create a table from all_linked_requests that has all columns EXCEPT those that have potential PII. This table is
# subsequently exported to WPRDC
drop_pii_query = f"""
CREATE TABLE {os.environ['GCLOUD_PROJECT']}.qalert.wprdc_export_scrubbed AS
(SELECT {COLS_IN_ORDER} except ({PII_FIELDS})
FROM {os.environ["GCLOUD_PROJECT"]}.qalert.all_linked_requests)
"""
qalert_requests_drop_pii_for_export = BigQueryOperator(
        task_id = 'qalert_requests_drop_pii_for_export',
        sql = drop_pii_query,
        use_legacy_sql = False,
        dag = dag
)

# Export table as CSV to WPRDC bucket
qalert_wprdc_export = BigQueryToCloudStorageOperator(
        task_id = 'qalert_wprdc_export',
        source_project_dataset_table = f"{os.environ['GCLOUD_PROJECT']}:qalert.wprdc_export_scrubbed",
        destination_cloud_storage_uris = [f"gs://{os.environ['GCS_PREFIX']}_wprdc/qalert_requests_" + "{{ ds }}.csv"],
        dag = dag
)

# Clean up
qalert_beam_cleanup = BashOperator(
        task_id = 'qalert_beam_cleanup',
        bash_command = airflow_utils.beam_cleanup_statement(f"{os.environ['GCS_PREFIX']}_qalert"),
        dag = dag
)

qalert_requests_gcs >> qalert_requests_dataflow >> qalert_requests_bq >> qalert_requests_format_dedupe >> \
qalert_requests_city_limits >> qalert_requests_geojoin >> qalert_requests_merge_new_tickets >> \
qalert_requests_split_new_parents >> qalert_requests_split_new_children >> \
qalert_requests_append_new_parent_tickets >> qalert_requests_update_parent_tickets >> \
qalert_requests_drop_pii_for_export >> qalert_wprdc_export >> qalert_beam_cleanup
