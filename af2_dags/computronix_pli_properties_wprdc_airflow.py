from __future__ import absolute_import

import os
import datetime
import time

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator

from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_year, get_ds_month, get_ds_day, default_args


now = datetime.date.today()
unix_date = time.mktime(now.timetuple())


# TODO: When Airflow 2.0 is released, upgrade the package, sub in DataFlowPythonOperator for BashOperator,
# and pass the argument 'py_interpreter=python3'

dag = DAG(
    'computronix_pli_properties',
    default_args=default_args,
    schedule_interval='@daily',
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year, 'get_ds_day': get_ds_day}
)


# initialize gcs locations
bucket = f"gs://{os.environ['GCS_PREFIX']}_computronix"
dataset = "pli_properties_wprdc"
path = "{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ ds|get_ds_day }}/{{ run_id }}"
json_loc = f"{path}_properties.json"
avro_loc = f"avro_output/{path}/"


# Run gcs_loader
exec_gcs = f"python {os.environ['GCS_LOADER_PATH']}/computronix_pli_properties_wprdc_gcs.py"
gcs_loader = BashOperator(
        task_id = 'gcs_loader',
        bash_command = f"{exec_gcs} --output_arg {dataset}/{json_loc}",
        dag = dag
)


# Run DF
exec_df = f"python {os.environ['DATAFLOW_SCRIPT_PATH']}/computronix_pli_properties_wprdc_dataflow.py"
dataflow = BashOperator(
        task_id = 'dataflow',
        bash_command = f"{exec_df} --input {bucket}/{dataset}/{json_loc} --avro_output {bucket}/{dataset}/{avro_loc}",
        dag = dag
)


# Load AVRO data produced by dataflow_script into BQ temp table
gcs_to_bq = GoogleCloudStorageToBigQueryOperator(
        task_id = 'gcs_to_bq',
        destination_project_dataset_table = f"{os.environ['GCLOUD_PROJECT']}:computronix.pli_permits_wprdc",
        bucket = f"{os.environ['GCS_PREFIX']}_computronix",
        source_objects = [f"{dataset}/{avro_loc}*.avro"],
        write_disposition = 'WRITE_TRUNCATE',
        create_disposition = 'CREATE_IF_NEEDED',
        source_format = 'AVRO',
        autodetect = True,
        dag = dag
)


# join the carte_id vals to the corresponding lat/long
query_join = F"""
CREATE OR REPLACE TABLE `{os.environ["GCLOUD_PROJECT"]}.computronix.pli_con_permits_wprdc` AS
SELECT 
         
         blah blah
         
         
FROM `{os.environ["GCLOUD_PROJECT"]}.computronix.gis_street_closures` sc
JOIN `{os.environ["GCLOUD_PROJECT"]}.timebound_geography.carte_id_street_segment_coords` lalo 
ON sc.carte_id = lalo.carte_id  
"""
join_coords = BigQueryOperator(
        task_id = 'join_coords',
        sql = query_join,
        bigquery_conn_id='google_cloud_default',
        use_legacy_sql = False,
        dag = dag
)


# Export table as CSV to WPRDC bucket (file name is the date. path contains the date info)
csv_file_name = f"{path}"
dest_bucket = f"gs://{os.environ['GCS_PREFIX']}_wprdc/domi_street_closures/street_segments/"
wprdc_export = BigQueryToCloudStorageOperator(
        task_id = 'wprdc_export',
        source_project_dataset_table = f"{os.environ['GCLOUD_PROJECT']}.computronix.gis_street_closures",
        destination_cloud_storage_uris = [f"{dest_bucket}{csv_file_name}.csv"],
        bigquery_conn_id='google_cloud_default',
        dag = dag
)


# Export table as CSV to DOMI bucket (file name is the date. path contains the date info)
csv_file_name = f"{path}"
dest_bucket = f"gs://{os.environ['GCS_PREFIX']}_domi/domi_street_closures/street_segments/"
domi_export = BigQueryToCloudStorageOperator(
        task_id = 'domi_export',
        source_project_dataset_table = f"{os.environ['GCLOUD_PROJECT']}.computronix.gis_street_closures",
        destination_cloud_storage_uris = [f"{dest_bucket}{csv_file_name}.csv"],
        bigquery_conn_id='google_cloud_default',
        dag = dag
)


# remove inactive permits
query_filter = F"""
CREATE OR REPLACE TABLE `{os.environ["GCLOUD_PROJECT"]}.computronix.gis_active_street_closures` AS
SELECT 
 *
FROM `{os.environ["GCLOUD_PROJECT"]}.computronix.gis_street_closures` 
WHERE from_date_UNIX <= {unix_date} AND to_date_unix >= {unix_date}
"""
filter_inactive = BigQueryOperator(
        task_id = 'filter_inactive',
        sql = query_filter,
        bigquery_conn_id='google_cloud_default',
        use_legacy_sql = False,
        dag = dag
)


# Export table as CSV to data-bridGIS bucket (This csv will be converted to json in the next step)
dest_bucket = f"gs://{os.environ['GIS_PREFIX']}_domi_street_closures/"
gis_csv_export = BigQueryToCloudStorageOperator(
        task_id = 'gis_csv_export',
        source_project_dataset_table = f"{os.environ['GCLOUD_PROJECT']}.computronix.gis_active_street_closures",
        destination_cloud_storage_uris = [f"{dest_bucket}active_closures.csv"],
        bigquery_conn_id='google_cloud_default',
        dag = dag
)


# Convert csv to geo enriched json
input_bucket = 'pghpa_gis_domi_street_closures'
input_blob = 'active_closures.csv'
output_bucket = 'pghpa_gis_domi_street_closures'
exec_conv = f"python {os.environ['DAG_SUBROUTINE_PATH']}/conv_coords_upload_json.py"
run_args = F"--input_bucket {input_bucket} --input_blob {input_blob} --output_bucket {output_bucket}"
json_conv = BashOperator(
        task_id = 'json_conv',
        bash_command = f"{exec_conv} {run_args}",
        dag = dag
)


beam_cleanup = BashOperator(
    task_id='beam_cleanup',
    bash_command=airflow_utils.beam_cleanup_statement('{}_computronix'.format(os.environ['GCS_PREFIX'])),
    dag=dag
)

# branching DAG splits after the gcs_to_bq stage and converges back at beam_cleanup
gcs_loader >> dataflow >> gcs_to_bq >> join_coords
join_coords >> wprdc_export >> beam_cleanup
join_coords >> domi_export >> beam_cleanup
join_coords >> filter_inactive >> gis_csv_export >> json_conv >> beam_cleanup