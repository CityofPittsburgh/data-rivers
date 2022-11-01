from __future__ import absolute_import

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_month, get_ds_year, get_ds_day, default_args, build_revgeo_time_bound_query

COLS_IN_ORDER = """id, department, status, entry_date_UTC, entry_date_EST, entry_date_UNIX, actual_start_date_UTC, 
actual_start_date_EST, actual_start_date_UNIX, actual_stop_date_UTC, actual_stop_date_EST, actual_stop_date_UNIX, 
labor_cost, equipment_cost, material_cost, labor_hours, request_issue, request_department, request_location, 
asset_id, asset_type, task_description, task_notes, neighborhood_name, council_district, ward, police_zone, 
fire_zone, dpw_streets, dpw_enviro, dpw_parks, lat, long"""

# This DAG will perform a pull of all work tasks entered into the Cartegraph application every 3 days
# and enrich the data with additional location details

dag = DAG(
    'cartegraph_tasks',
    default_args=default_args,
    schedule_interval='0 15 */3 * *',
    start_date=datetime(2022, 11, 4),
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year,
                          'get_ds_day': get_ds_day}
)

# initialize gcs locations
bucket = f"gs://{os.environ['GCS_PREFIX']}_cartegraph"
dataset = "tasks"
exec_date = "{{ ds }}"
path = "{{ ds|get_ds_year }}/{{ ds|get_ds_month }}"
json_loc = f"{dataset}/{path}/{exec_date}_tasks.json"
avro_loc = f"{dataset}/avro_output/{path}/" + "{{ run_id }}"

cartegraph_gcs = BashOperator(
    task_id='cartegraph_gcs',
    bash_command=f"python {os.environ['GCS_LOADER_PATH']}/cartegraph_tasks_gcs.py --output_arg {json_loc}",
    dag=dag
)

cartegraph_dataflow = BashOperator(
        task_id = 'cartegraph_dataflow',
        bash_command = f"python {os.environ['DATAFLOW_SCRIPT_PATH']}/cartegraph_tasks_dataflow.py "
                       f"--input {bucket}/{json_loc} --avro_output {bucket}/{avro_loc}",
        dag = dag
)

cartegraph_bq_load = GoogleCloudStorageToBigQueryOperator(
        task_id = 'cartegraph_bq_load',
        destination_project_dataset_table = f"{os.environ['GCLOUD_PROJECT']}.cartegraph.incoming_tasks",
        bucket = f"{os.environ['GCS_PREFIX']}_cartegraph",
        source_objects = [f"{avro_loc}*.avro"],
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
        source_format = 'AVRO',
        autodetect = True,
        bigquery_conn_id='google_cloud_default',
        dag = dag
)

query_format_dedupe = f"""
CREATE OR REPLACE TABLE `{os.environ['GCLOUD_PROJECT']}.cartegraph.incoming_tasks` AS
WITH formatted  AS 
    (
    SELECT 
        DISTINCT * EXCEPT (lat, long),
        CAST(lat AS FLOAT64) AS lat,
        CAST(long AS FLOAT64) AS long,
    FROM 
        {os.environ['GCLOUD_PROJECT']}.cartegraph.incoming_tasks
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

# Join all the geo information (e.g. DPW districts, etc) to the new data
query_geo_join = build_revgeo_time_bound_query('cartegraph', 'incoming_tasks', 'incoming_enriched',
                                               'actual_start_date_UTC', 'id', 'lat', 'long')
geojoin = BigQueryOperator(
        task_id = 'geojoin',
        sql = query_geo_join,
        bigquery_conn_id='google_cloud_default',
        use_legacy_sql = False,
        dag = dag
)

query_insert_tasks = f"""
-- This query check that a task has never been seen before (checks all_tasks) before inserting into the growing table.

INSERT INTO `{os.environ['GCLOUD_PROJECT']}.cartegraph.all_tasks`
(
SELECT
    {COLS_IN_ORDER}
FROM
    `{os.environ['GCLOUD_PROJECT']}.cartegraph.incoming_enriched`
WHERE id NOT IN (SELECT id FROM `{os.environ['GCLOUD_PROJECT']}.cartegraph.all_tasks`)
);
"""
insert_new_tasks = BigQueryOperator(
        task_id = 'insert_new_tasks',
        sql = query_insert_tasks,
        bigquery_conn_id='google_cloud_default',
        use_legacy_sql = False,
        dag = dag
)

beam_cleanup = BashOperator(
        task_id = 'cartegraph_beam_cleanup',
        bash_command = airflow_utils.beam_cleanup_statement(f"{os.environ['GCS_PREFIX']}_cartegraph_tasks"),
        dag = dag
)

cartegraph_gcs >> cartegraph_dataflow >> cartegraph_bq_load >> format_dedupe >> \
geojoin >> insert_new_tasks >> beam_cleanup