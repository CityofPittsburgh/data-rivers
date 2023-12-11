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

from dependencies.bq_queries.cx_pli import condemned_dead_end_properties as q

dag = DAG(
    'computronix_pli_condemned_dead_end_properties',
    default_args=default_args,
    schedule_interval='5 4 * * *',
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


# Load AVRO data produced by dataflow_script into BQ staging table
gcs_to_bq = GoogleCloudStorageToBigQueryOperator(
        task_id = 'gcs_to_bq',
        destination_project_dataset_table = F"{os.environ['GCLOUD_PROJECT']}:computronix.incoming_pli_program_inspection_properties",
        bucket = f"{os.environ['GCS_PREFIX']}_computronix",
        source_objects = [f"{dataset}/{avro_loc}*.avro"],
        write_disposition = 'WRITE_TRUNCATE',
        create_disposition = 'CREATE_IF_NEEDED',
        source_format = 'AVRO',
        autodetect = True,
        dag = dag
)


# union all  new records with the existing records that did not get pulled in if an api request error occurred
combine_incoming_existing = BigQueryOperator(
        task_id = 'combine_incoming_existing',
        sql = q.combine_incoming_existing_recs(),
        bigquery_conn_id='google_cloud_default',
        use_legacy_sql = False,
        dag = dag
)


# seperate all condemened and dead end records for pli export (2 tables)
seperate_pli_con_dead_end = BigQueryOperator(
        task_id = 'seperate_con_dead_end',
        sql = q.create_pli_exp_active_tables(),
        bigquery_conn_id='google_cloud_default',
        use_legacy_sql = False,
        dag = dag
)


# Export table 1 as CSV to PLI bucket (file name is the date. path contains the date info)
csv_file_name = f"{path}"
dest_bucket = f"gs://{os.environ['GCS_PREFIX']}_pli/condemned_properties/"
pli_export_condemned = BigQueryToCloudStorageOperator(
        task_id = 'pli_export_condemned',
        source_project_dataset_table = f"{os.environ['GCLOUD_PROJECT']}.computronix.pli_active_condemned_properties",
        destination_cloud_storage_uris = [f"{dest_bucket}{csv_file_name}.csv"],
        bigquery_conn_id='google_cloud_default',
        dag = dag
)


# Export table 2 as CSV to PLI bucket (file name is the date. path contains the date info)
csv_file_name = f"{path}"
dest_bucket = f"gs://{os.environ['GCS_PREFIX']}_pli/dead_end_properties/"
pli_export_dead_end = BigQueryToCloudStorageOperator(
        task_id = 'pli_export_dead_end',
        source_project_dataset_table = f"{os.environ['GCLOUD_PROJECT']}.computronix.pli_active_dead_end_properties",
        destination_cloud_storage_uris = [f"{dest_bucket}{csv_file_name}.csv"],
        bigquery_conn_id='google_cloud_default',
        dag = dag
)


# Export table as CSV to WPRDC bucket (file name is the date. path contains the date info)
csv_file_name = f"{path}"
dest_bucket = f"gs://{os.environ['GCS_PREFIX']}_wprdc/pli/condemned_dead_end_properties/"
wprdc_export = BigQueryToCloudStorageOperator(
        task_id = 'wprdc_export',
        source_project_dataset_table = f"{os.environ['GCLOUD_PROJECT']}.computronix.pli_cde_properties",
        destination_cloud_storage_uris = [f"{dest_bucket}{csv_file_name}.csv"],
        bigquery_conn_id='google_cloud_default',
        dag = dag
)


# push table of the latest updates of ALL cde permits (not just active) data-bridGIS BQ
push_gis_latest_cde = BigQueryOperator(
        task_id = 'push_gis_latest_cde',
        sql = q.push_gis_latest_updates(),
        bigquery_conn_id='google_cloud_default',
        use_legacy_sql = False,
        dag = dag
)


beam_cleanup = BashOperator(
    task_id='beam_cleanup',
    bash_command=airflow_utils.beam_cleanup_statement('{}_computronix'.format(os.environ['GCS_PREFIX'])),
    dag=dag
)


gcs_loader >> dataflow >> gcs_to_bq >> combine_incoming_existing
combine_incoming_existing >> wprdc_export >> beam_cleanup
combine_incoming_existing >> seperate_pli_con_dead_end
seperate_pli_con_dead_end >> pli_export_condemned >> beam_cleanup
seperate_pli_con_dead_end >> pli_export_dead_end >> beam_cleanup
combine_incoming_existing >> push_gis_latest_cde >> beam_cleanup
