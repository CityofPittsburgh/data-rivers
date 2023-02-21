from __future__ import absolute_import

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_month, get_ds_year, get_ds_day, default_args

COLS_IN_ORDER = """id, name, average_daily_traffic, notes, owner, maintenance_responsibility, entry_date,
                   replaced_date, last_modified_date, retired_date, length_ft, width_ft, height_ft, weight_limit_tons,
                   left_sidewalk_width_ft, right_sidewalk_width_ft, use_type, travel_lanes, deck_type, 
                   deck_wearing_surface, deck_condition_rating, structure_type, substructure_condition_rating, 
                   superstructure_condition_rating, culvert_condition_rating, paint_type, number_of_spans,
                   scuppers_with_downspouts, scuppers_without_downspouts, has_drainage, walkway_maintenance_priority, 
                   total_cost, estimated_treatment_time_min, feature_crossed, crosses, street_name, park, 
                   neighborhood_name, neighborhood_name_2, council_district, council_district_2, lat, long, geometry"""

# This DAG will perform a pull of all bridges entered into the Cartegraph application every month
# and enrich the data with additional location details

dag = DAG(
    'cartegraph_bridges',
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2022, 11, 1),
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year,
                          'get_ds_day': get_ds_day}
)

# initialize gcs locations
bucket = f"gs://{os.environ['GCS_PREFIX']}_cartegraph"
dataset = "bridges"
exec_date = "{{ ds }}"
path = "{{ ds|get_ds_year }}"
json_loc = f"{dataset}/{path}/{exec_date}_bridges.json"
avro_loc = f"{dataset}/avro_output/{path}/" + "{{ run_id }}"

cartegraph_gcs = BashOperator(
    task_id='cartegraph_gcs',
    bash_command=f"python {os.environ['GCS_LOADER_PATH']}/cartegraph_assets_gcs.py --output_arg {json_loc} "
                 f"--asset cgBridgesClass",
    dag=dag
)

cartegraph_dataflow = BashOperator(
        task_id = 'cartegraph_dataflow',
        bash_command = f"python {os.environ['DATAFLOW_SCRIPT_PATH']}/cartegraph_bridges_dataflow.py "
                       f"--input {bucket}/{json_loc} --avro_output {bucket}/{avro_loc}",
        dag = dag
)

cartegraph_bq_load = GoogleCloudStorageToBigQueryOperator(
        task_id = 'cartegraph_bq_load',
        destination_project_dataset_table = f"{os.environ['GCLOUD_PROJECT']}.cartegraph.bridges",
        bucket = f"{os.environ['GCS_PREFIX']}_cartegraph",
        source_objects = [f"{avro_loc}*.avro"],
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
        source_format = 'AVRO',
        autodetect = True,
        bigquery_conn_id='google_cloud_default',
        dag = dag
)

query_format_table = f"""
CREATE OR REPLACE TABLE `{os.environ['GCLOUD_PROJECT']}.cartegraph.bridges` AS
WITH formatted  AS 
    (
    SELECT 
        * EXCEPT (geometry, lat, long),
        ST_GEOGFROMTEXT(geometry) AS geometry,
        CAST(lat AS FLOAT64) AS lat,
        CAST(long AS FLOAT64) AS long,
    FROM 
        {os.environ['GCLOUD_PROJECT']}.cartegraph.bridges
    )
-- drop the final column through slicing the string. final column is added in next query     
SELECT 
    {COLS_IN_ORDER} 
FROM 
    formatted
"""
format_table = BigQueryOperator(
    task_id='format_table',
    sql=query_format_table,
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

beam_cleanup = BashOperator(
        task_id = 'cartegraph_beam_cleanup',
        bash_command = airflow_utils.beam_cleanup_statement(f"{os.environ['GCS_PREFIX']}_cartegraph_bridges"),
        dag = dag
)

cartegraph_gcs >> cartegraph_dataflow >> cartegraph_bq_load >> format_table >> beam_cleanup