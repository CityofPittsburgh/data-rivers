from __future__ import absolute_import

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_year, get_ds_month, get_ds_day, default_args
import dependencies.bq_queries.qscend.transform_enrich_requests as q

# The goal of this DAG is to extract details about the submitters of recent 311 requests and store them
# alongside ticket details in our BigQuery data warehouse so that our analyst team can display information
# about high service utilizers on a Looker Studio dashboard.

dag = DAG(
    'qalert_submitters',
    default_args=default_args,
    schedule_interval='20 * * * *',
    start_date=datetime(2023, 10, 31),
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year, 'get_ds_day': get_ds_day},
    max_active_runs=1,
    catchup=False
)

bucket = f"gs://{os.environ['GCS_PREFIX']}_qalert"
dataset = 'submitters'
path = "{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ ds|get_ds_day }}/{{ run_id }}"
json_loc = f"{path}_submitters.json"
hot_bucket = f"gs://{os.environ['GCS_PREFIX']}_hot_metal"
output_name = "311_utilizers"

# Run gcs_loader
submitters_gcs_loader = BashOperator(
    task_id='submitters_gcs_loader',
    bash_command=F"python {os.environ['DAGS_PATH']}/dependencies/gcs_loaders/qalert_submitters_gcs.py "
                 F"--output_arg {dataset}/{json_loc}",
    dag=dag
)

exec_df = f"python {os.environ['DATAFLOW_SCRIPT_PATH']}/qalert_submitters_dataflow.py"
submitters_dataflow = BashOperator(
    task_id='qalert_submitters_dataflow',
    bash_command=f"{exec_df} --input {bucket}/{dataset}/{json_loc} --avro_output {hot_bucket}/{output_name}",
    dag=dag
)

# Load AVRO data produced by dataflow_script into BQ temp table
submitters_gcs_to_bq = GoogleCloudStorageToBigQueryOperator(
    task_id='submitters_gcs_to_bq',
    destination_project_dataset_table=f"{os.environ['GCLOUD_PROJECT']}:qalert.latest_submitters",
    bucket=f"{os.environ['GCS_PREFIX']}_hot_metal",
    source_objects=[f"{output_name}*.avro"],
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    source_format='AVRO',
    autodetect=True,
    bigquery_conn_id='google_cloud_default',
    dag=dag
)

join_submitter_to_request = BigQueryOperator(
    task_id='join_submitter_to_request',
    sql=q.join_submitter_to_request(),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

delete_submitter_avro = BashOperator(
    task_id='delete_submitter_avro',
    bash_command=f"gsutil rm -r {hot_bucket}/{output_name}*.avro",
    dag=dag
)

submitters_beam_cleanup = BashOperator(
    task_id='submitters_beam_cleanup',
    bash_command=airflow_utils.beam_cleanup_statement(f"{os.environ['GCS_PREFIX']}_qalert"),
    dag=dag
)

submitters_gcs_loader >> submitters_dataflow >> submitters_gcs_to_bq >> join_submitter_to_request >> \
    delete_submitter_avro >> submitters_beam_cleanup
