from __future__ import absolute_import

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_month, get_ds_year, get_ds_day, default_args

COLS_IN_ORDER = """id, name, type, description, notes, primary_user, installed_date,
                   entry_date, last_modified_date, probability_of_failure_score, planned_renovations,
                   planned_intervention_level, planned_intervention_year, street_num, street_name,
                   zip, park, neighborhood_name, council_district, public_view, public_restrooms,
                   is_rentable, is_vacant, floor_count, floors_below_grade, foundation_type,
                   building_envelope, parking_type, ada_notes, ada_accessible_approach_entrance,
                   ada_accessible_to_goods_and_services, ada_additional_access, ada_usability_of_restrooms,
                   ada_assessment_date, total_cost, saving_opportunity, energy_renovation_cost_estimate,
                   replaced_date, replacement_cost_type, size_sq_foot, lat, long, geometry"""

# This DAG will perform a daily pull of all facilities entered into the Cartegraph application
# and enrich the data with additional location details

dag = DAG(
    'cartegraph_facilities',
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2022, 2, 10),
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year,
                          'get_ds_day': get_ds_day}
)

# initialize gcs locations
bucket = f"gs://{os.environ['GCS_PREFIX']}_cartegraph"
dataset = "facilities"
path = "{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ run_id }}"
json_loc = f"{dataset}/{path}_{dataset}.json"
avro_loc = f"{dataset}/avro_output/{path}/"

cartegraph_gcs = BashOperator(
    task_id='cartegraph_gcs',
    bash_command=f"python {os.environ['GCS_LOADER_PATH']}/cartegraph_assets_gcs.py --output_arg {json_loc} "
                 f"--asset cgFacilitiesClass",
    dag=dag
)

cartegraph_dataflow = BashOperator(
        task_id='cartegraph_dataflow',
        bash_command=f"python {os.environ['DATAFLOW_SCRIPT_PATH']}/cartegraph_facilities_dataflow.py "
                     f"--input {bucket}/{json_loc} --avro_output {bucket}/{avro_loc}",
        dag=dag
)

cartegraph_bq_load = GoogleCloudStorageToBigQueryOperator(
        task_id = 'cartegraph_bq_load',
        destination_project_dataset_table = f"{os.environ['GCLOUD_PROJECT']}.cartegraph.facilities",
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
CREATE OR REPLACE TABLE `{os.environ['GCLOUD_PROJECT']}.cartegraph.facilities` AS
WITH formatted  AS 
    (
    SELECT 
        * EXCEPT (geometry, lat, long),
        ST_GEOGFROMTEXT(geometry) AS geometry,
        CAST(lat AS FLOAT64) AS lat,
        CAST(long AS FLOAT64) AS long,
    FROM 
        {os.environ['GCLOUD_PROJECT']}.cartegraph.facilities
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
        task_id='cartegraph_beam_cleanup',
        bash_command=airflow_utils.beam_cleanup_statement(f"{os.environ['GCS_PREFIX']}_cartegraph_facilities"),
        dag=dag
)

cartegraph_gcs >> cartegraph_dataflow >> cartegraph_bq_load >> format_table >> beam_cleanup
