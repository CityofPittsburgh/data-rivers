from __future__ import absolute_import

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_month, get_ds_year, get_ds_day, default_args

COLS_IN_ORDER = """id, type, manufacturer, description, notes, playground, primary_attachment, ada_accessible, 
                   address_num, street_name, city, safety_surface_type, entry_date, installed_date, last_modified_date, 
                   replaced_date, retired_date, inactive, total_cost, lat, long"""

# This DAG will perform a pull of all playground equipment entered into the Cartegraph application every month,
# as this data is currently not being tracked by WPRDC or the GIS team.

dag = DAG(
    'cartegraph_playground_equipment',
    default_args=default_args,
    schedule_interval="@monthly",
    start_date=datetime(2023, 1, 1),
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year,
                          'get_ds_day': get_ds_day}
)


# initialize gcs locations
bucket = f"gs://{os.environ['GCS_PREFIX']}_cartegraph"
dataset = "playground_equipment"
exec_date = "{{ ds }}"
path = "{{ ds|get_ds_year }}"
json_loc = f"{dataset}/{path}/{exec_date}_playground_equipment.json"
avro_loc = f"{dataset}/avro_output/{path}/" + "{{ run_id }}"

cartegraph_gcs = BashOperator(
    task_id='cartegraph_gcs',
    bash_command=f"python {os.environ['GCS_LOADER_PATH']}/cartegraph_assets_gcs.py --output_arg {json_loc} "
                 f"--asset PlaygroundEquipmentClass",
    dag=dag
)


cartegraph_dataflow = BashOperator(
        task_id = 'cartegraph_dataflow',
        bash_command = f"python {os.environ['DATAFLOW_SCRIPT_PATH']}/cartegraph_playground_equipment_dataflow.py "
                       f"--input {bucket}/{json_loc} --avro_output {bucket}/{avro_loc}",
        dag = dag
)


cartegraph_bq_load = GoogleCloudStorageToBigQueryOperator(
        task_id = 'cartegraph_bq_load',
        destination_project_dataset_table = f"{os.environ['GCLOUD_PROJECT']}.cartegraph.playground_equipment",
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
CREATE OR REPLACE TABLE `{os.environ['GCLOUD_PROJECT']}.cartegraph.playground_equipment` AS
WITH formatted  AS 
    (
    SELECT 
        * EXCEPT (lat, long),
        CAST(lat AS FLOAT64) AS lat,
        CAST(long AS FLOAT64) AS long,
    FROM 
        {os.environ['GCLOUD_PROJECT']}.cartegraph.playground_equipment
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


# Export table as CSV to WPRDC bucket
# file name is the month and year. Template variable contains the month, path contains the year
csv_file_name = "{{ ds|get_ds_month }}-"+path
dest_bucket = f"gs://{os.environ['GCS_PREFIX']}_wprdc/cartegraph_playground_equipment/"
wprdc_export = BigQueryToCloudStorageOperator(
        task_id = 'wprdc_export',
        source_project_dataset_table = f"{os.environ['GCLOUD_PROJECT']}.cartegraph.playground_equipment",
        destination_cloud_storage_uris = [f"{dest_bucket}{csv_file_name}.csv"],
        dag = dag
)

beam_cleanup = BashOperator(
        task_id = 'cartegraph_beam_cleanup',
        bash_command = airflow_utils.beam_cleanup_statement(f"{os.environ['GCS_PREFIX']}_cartegraph"),
        dag = dag
)

cartegraph_gcs >> cartegraph_dataflow >> cartegraph_bq_load >> format_table >> wprdc_export >> beam_cleanup