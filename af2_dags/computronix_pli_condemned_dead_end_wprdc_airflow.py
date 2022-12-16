from __future__ import absolute_import

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator

from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_year, get_ds_month, get_ds_day, default_args


# TODO: When Airflow 2.0 is released, upgrade the package, sub in DataFlowPythonOperator for BashOperator,
# and pass the argument 'py_interpreter=python3'

dag = DAG(
    'computronix_pli_condemned_dead_end_properties',
    default_args=default_args,
    schedule_interval='@daily',
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year, 'get_ds_day': get_ds_day},
    start_date=datetime(2022, 12, 16),
    catchup = False
)


# initialize gcs locations
bucket = f"gs://{os.environ['GCS_PREFIX']}_computronix"
dataset = "pli_condemned_dead_end_properties_wprdc"
path = "{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ ds|get_ds_day }}/{{ run_id }}"
json_loc = f"{path}_condemned_dead_end_properties.json"
avro_loc = f"avro_output/{path}/"


# Run gcs_loader
exec_gcs = f"python {os.environ['GCS_LOADER_PATH']}/computronix_pli_condemned_dead_end_properties_wprdc_gcs.py"
gcs_loader = BashOperator(
        task_id = 'gcs_loader',
        bash_command = f"{exec_gcs} --output_arg {dataset}/{json_loc}",
        dag = dag
)


# Run DF
exec_df = f"python {os.environ['DATAFLOW_SCRIPT_PATH']}/computronix_pli_condemned_dead_end_properties_wprdc_dataflow.py"
dataflow = BashOperator(
        task_id = 'dataflow',
        bash_command = f"{exec_df} --input {bucket}/{dataset}/{json_loc} --avro_output {bucket}/{dataset}/{avro_loc}",
        dag = dag
)


# Load AVRO data produced by dataflow_script into BQ temp table
gcs_to_bq = GoogleCloudStorageToBigQueryOperator(
        task_id = 'gcs_to_bq',
        destination_project_dataset_table =f"{os.environ['GCLOUD_PROJECT']}:computronix.pli_program_inspection_properties",
        bucket = f"{os.environ['GCS_PREFIX']}_computronix",
        source_objects = [f"{dataset}/{avro_loc}*.avro"],
        write_disposition = 'WRITE_TRUNCATE',
        create_disposition = 'CREATE_IF_NEEDED',
        source_format = 'AVRO',
        autodetect = True,
        dag = dag
)


# seperate the condemned properties into a table
query_condemned = F"""
CREATE OR REPLACE TABLE 
`{os.environ['GCLOUD_PROJECT']}.computronix.pli_condemned_properties` AS
SELECT 
    *      
FROM `{os.environ['GCLOUD_PROJECT']}.computronix.pli_program_inspection_properties`
WHERE insp_type_desc LIKE 'Condemned Property' AND insp_status NOT LIKE 'Inactive'
"""
seperate_condemned = BigQueryOperator(
        task_id = 'seperate_condemned',
        sql = query_condemned,
        bigquery_conn_id='google_cloud_default',
        use_legacy_sql = False,
        dag = dag
)


# seperate the dead end properties into a table
query_dead_end = F"""
CREATE OR REPLACE TABLE 
`{os.environ['GCLOUD_PROJECT']}.computronix.pli_dead_end_properties` AS
SELECT 
    * EXCEPT (latest_inspec_result, latest_inspec_score)  
FROM `{os.environ['GCLOUD_PROJECT']}.computronix.pli_program_inspection_properties`
WHERE insp_type_desc LIKE 'Dead End Property' AND insp_status NOT LIKE 'Inactive'
"""
seperate_dead_end = BigQueryOperator(
        task_id = 'seperate_dead_end',
        sql = query_dead_end,
        bigquery_conn_id='google_cloud_default',
        use_legacy_sql = False,
        dag = dag
)


# Export table 1 as CSV to WPRDC bucket (file name is the date. path contains the date info)
csv_file_name = f"{path}"
dest_bucket = f"gs://{os.environ['GCS_PREFIX']}_wprdc/pli/condemned_properties/"
wprdc_export_condemned = BigQueryToCloudStorageOperator(
        task_id = 'wprdc_export_condemned',
        source_project_dataset_table = f"{os.environ['GCLOUD_PROJECT']}.computronix.pli_condemned_properties",
        destination_cloud_storage_uris = [f"{dest_bucket}{csv_file_name}.csv"],
        bigquery_conn_id='google_cloud_default',
        dag = dag
)


# Export table 2 as CSV to WPRDC bucket (file name is the date. path contains the date info)
csv_file_name = f"{path}"
dest_bucket = f"gs://{os.environ['GCS_PREFIX']}_wprdc/pli/dead_end_properties/"
wprdc_export_dead_end = BigQueryToCloudStorageOperator(
        task_id = 'wprdc_export_dead_end',
        source_project_dataset_table = f"{os.environ['GCLOUD_PROJECT']}.computronix.pli_dead_end_properties",
        destination_cloud_storage_uris = [f"{dest_bucket}{csv_file_name}.csv"],
        bigquery_conn_id='google_cloud_default',
        dag = dag
)


# Export table 1 as CSV to PLI bucket (file name is the date. path contains the date info)
csv_file_name = f"{path}"
dest_bucket = f"gs://{os.environ['GCS_PREFIX']}_pli/condemned_properties/"
pli_export_condemned = BigQueryToCloudStorageOperator(
        task_id = 'pli_export_condemned',
        source_project_dataset_table = f"{os.environ['GCLOUD_PROJECT']}.computronix.pli_condemned_properties",
        destination_cloud_storage_uris = [f"{dest_bucket}{csv_file_name}.csv"],
        bigquery_conn_id='google_cloud_default',
        dag = dag
)


# Export table 2 as CSV to PLI bucket (file name is the date. path contains the date info)
csv_file_name = f"{path}"
dest_bucket = f"gs://{os.environ['GCS_PREFIX']}_pli/dead_end_properties/"
pli_export_dead_end = BigQueryToCloudStorageOperator(
        task_id = 'pli_export_dead_end',
        source_project_dataset_table = f"{os.environ['GCLOUD_PROJECT']}.computronix.pli_dead_end_properties",
        destination_cloud_storage_uris = [f"{dest_bucket}{csv_file_name}.csv"],
        bigquery_conn_id='google_cloud_default',
        dag = dag
)


beam_cleanup = BashOperator(
    task_id='beam_cleanup',
    bash_command=airflow_utils.beam_cleanup_statement('{}_computronix'.format(os.environ['GCS_PREFIX'])),
    dag=dag
)

# branching DAG splits after the gcs_to_bq stage and converges back at beam_cleanup
gcs_loader >> dataflow >> gcs_to_bq
gcs_to_bq >> seperate_condemned
gcs_to_bq >> seperate_dead_end
seperate_condemned >> wprdc_export_condemned >> beam_cleanup
seperate_condemned >> pli_export_condemned >> beam_cleanup
seperate_dead_end >> wprdc_export_dead_end >> beam_cleanup
seperate_dead_end >> pli_export_dead_end >> beam_cleanup
