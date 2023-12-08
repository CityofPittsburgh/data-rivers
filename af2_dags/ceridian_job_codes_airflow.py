from __future__ import absolute_import

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_month, get_ds_year, get_ds_day, default_args, perform_data_quality_check

from dependencies.bq_queries.employee_admin import ceridian_admin as c_q
from dependencies.bq_queries import general_queries as g_q

# The goal of this DAG is to extract descriptive details about each job title held by City personnel. The data
# will be merged with employee demographic info to create an Equal Opportunity report, displaying racial/gender
# demographics for each salary range and job category.

dag = DAG(
    'ceridian_job_codes',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2023, 12, 8),
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year,
                          'get_ds_day': get_ds_day}
)

# initialize gcs locations
bucket = f"gs://{os.environ['GCS_PREFIX']}_ceridian"
dataset = "job_codes"
exec_date = "{{ ds }}"
path = "{{ ds|get_ds_year }}/{{ ds|get_ds_month }}"
json_loc = f"{dataset}/{path}/{exec_date}_jobs.json"
hot_bucket = f"gs://{os.environ['GCS_PREFIX']}_hot_metal"
output_name = f"{dataset}_report"
dq_checker = "ceridian_job_functions"

ceridian_jobs_gcs = BashOperator(
    task_id='ceridian_jobs_gcs',
    bash_command=f"python {os.environ['GCS_LOADER_PATH']}/ceridian_job_codes_gcs.py --output_arg {json_loc}",
    dag=dag
)

ceridian_jobs_dataflow = BashOperator(
    task_id='ceridian_jobs_dataflow',
    bash_command=f"python {os.environ['DATAFLOW_SCRIPT_PATH']}/ceridian_job_codes_dataflow.py "
                 f"--input {bucket}/{json_loc} --avro_output {hot_bucket}/{output_name}",
    dag=dag
)

ceridian_jobs_bq_load = GoogleCloudStorageToBigQueryOperator(
    task_id='ceridian_jobs_bq_load',
    destination_project_dataset_table=f"{os.environ['GCLOUD_PROJECT']}.ceridian.job_details",
    bucket=f"{os.environ['GCS_PREFIX']}_hot_metal",
    source_objects=[f"{output_name}*.avro"],
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    source_format='AVRO',
    autodetect=True,
    bigquery_conn_id='google_cloud_default',
    dag=dag
)

build_eeo4_report = BigQueryOperator(
    task_id='build_eeo4_report',
    sql=c_q.build_eeo4_report(),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

create_data_quality_table = BigQueryOperator(
    task_id='create_data_quality_table',
    sql=g_q.build_data_quality_table('ceridian', dq_checker, 'job_details', 'job_function'),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

export_data_quality = BigQueryToCloudStorageOperator(
    task_id='export_data_quality',
    source_project_dataset_table=f"{os.environ['GCLOUD_PROJECT']}.data_quality_check.{dq_checker}",
    destination_cloud_storage_uris=[f"gs://{os.environ['GCS_PREFIX']}_data_quality_check/TEMP_{dq_checker}.json"],
    export_format='NEWLINE_DELIMITED_JSON',
    bigquery_conn_id='google_cloud_default',
    dag=dag
)

data_quality_check = PythonOperator(
    task_id='data_quality_check',
    python_callable=perform_data_quality_check,
    op_kwargs={"file_name": f"{dq_checker}.json"},
    dag=dag
)

beam_cleanup = BashOperator(
    task_id='ceridian_beam_cleanup',
    bash_command=airflow_utils.beam_cleanup_statement(f"{os.environ['GCS_PREFIX']}_ceridian"),
    dag=dag
)

ceridian_jobs_gcs >> ceridian_jobs_dataflow >> ceridian_jobs_bq_load >> build_eeo4_report >> beam_cleanup
ceridian_jobs_gcs >> ceridian_jobs_dataflow >> ceridian_jobs_bq_load >> create_data_quality_table >> \
    export_data_quality >> data_quality_check >> beam_cleanup
