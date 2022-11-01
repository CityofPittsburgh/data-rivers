from __future__ import absolute_import

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_month, get_ds_year, get_ds_day, \
    default_args, build_percentage_table_query

# The goal of this DAG is to perform a daily pull of basic demographic information for each
# City of Pittsburgh employee via the Ceridian Dayforce API. This  data will be stored securely
# in Data Rivers and used for a few projects. For one, we will use the list of employees returned
# by the API to determine who has left the City payroll so that we can stop keeping track of
# their COVID vaccination status. Additionally, we will use de-identified race, sex, and union
# membership totals to display on Dashburgh. This will give the public insight on the demographics
# of the city government and how it compares to the demographics of the city as a whole.

dag = DAG(
    'ceridian',
    default_args=default_args,
    schedule_interval='@weekly',
    start_date=datetime(2022, 10, 21),
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year,
                          'get_ds_day': get_ds_day}
)

# initialize gcs locations
bucket = f"gs://{os.environ['GCS_PREFIX']}_ceridian"
dataset = "employees"
exec_date = "{{ ds }}"
path = "{{ ds|get_ds_year }}/{{ ds|get_ds_month }}"
json_loc = f"{dataset}/{path}/{exec_date}_employees.json"
avro_loc = f"{dataset}/avro_output/{path}/" + "{{ run_id }}"

ceridian_gcs = BashOperator(
    task_id='ceridian_gcs',
    bash_command=f"python {os.environ['GCS_LOADER_PATH']}/ceridian_gcs.py --output_arg {json_loc}",
    dag=dag
)

ceridian_dataflow = BashOperator(
        task_id = 'ceridian_dataflow',
        bash_command = f"python {os.environ['DATAFLOW_SCRIPT_PATH']}/ceridian_dataflow.py --input {bucket}/{json_loc} "
                       f"--avro_output {bucket}/{avro_loc}",
        dag = dag
)

ceridian_bq_load = GoogleCloudStorageToBigQueryOperator(
        task_id = 'ceridian_bq_load',
        destination_project_dataset_table = f"{os.environ['GCLOUD_PROJECT']}.ceridian.active_employees",
        bucket = f"{os.environ['GCS_PREFIX']}_ceridian",
        source_objects = [f"{avro_loc}*.avro"],
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
        source_format = 'AVRO',
        autodetect = True,
        bigquery_conn_id='google_cloud_default',
        dag = dag
)

# People marked as belonging to the department of 'Non-Employee Benefits are individuals who have negotiated to have
# their benefits administered by the City of Pittsburgh and should not be reflected in any employee data
query_remove_rows = f"""
DELETE FROM `{os.environ['GCLOUD_PROJECT']}.ceridian.active_employees` 
WHERE dept = 'Non-Employee Benefits'"""
remove_non_employees = BigQueryOperator(
        task_id = 'remove_non_employees',
        sql = query_remove_rows,
        bigquery_conn_id='google_cloud_default',
        use_legacy_sql = False,
        dag = dag
)

gender_table = 'employee_vs_gen_pop_gender_comp'
gender_pct_field = 'gender'
categories = ['City Employee', 'Overall City']
gender_hardcoded_vals = [{gender_pct_field: 'M', 'percentage': 00.49},
                         {gender_pct_field: 'F', 'percentage': 00.51}]
query_gender_comp = build_percentage_table_query('ceridian', 'active_employees', gender_table,
                                                 False, 'employee_num', gender_pct_field,
                                                 categories, gender_hardcoded_vals)
create_gender_comp_table = BigQueryOperator(
        task_id = 'create_gender_comp_table',
        sql = query_gender_comp,
        bigquery_conn_id='google_cloud_default',
        use_legacy_sql = False,
        dag = dag
)

race_table = 'employee_vs_gen_pop_racial_comp'
race_pct_field = 'ethnicity'
race_hardcoded_vals = [{race_pct_field: 'White', 'percentage': 00.645},
                       {race_pct_field: 'Black or African American', 'percentage': 00.23},
                       {race_pct_field: 'Asian', 'percentage': 00.058},
                       {race_pct_field: 'Hispanic or Latino', 'percentage': 00.034},
                       {race_pct_field: 'American Indian or Alaska Native', 'percentage': 00.002},
                       {race_pct_field: 'Native Hawaiian or Other Pacific Islander', 'percentage': 00.001},
                       {race_pct_field: 'Two or More Races', 'percentage': 00.036}]
query_racial_comp = build_percentage_table_query('ceridian', 'active_employees', race_table,
                                                 False, 'employee_num', race_pct_field,
                                                 categories, race_hardcoded_vals)
create_racial_comp_table = BigQueryOperator(
        task_id = 'create_racial_comp_table',
        sql = query_racial_comp,
        bigquery_conn_id='google_cloud_default',
        use_legacy_sql = False,
        dag = dag
)

# Export employee table to Ceridian bucket as readable CSV
ceridian_export = BigQueryToCloudStorageOperator(
        task_id = 'ceridian_export',
        source_project_dataset_table = f"{os.environ['GCLOUD_PROJECT']}.{dataset}.active_employees",
        destination_cloud_storage_uris = [f"{bucket}/shared/ceridian_report.csv"],
        bigquery_conn_id='google_cloud_default',
        dag = dag
)

beam_cleanup = BashOperator(
        task_id = 'ceridian_beam_cleanup',
        bash_command = airflow_utils.beam_cleanup_statement(f"{os.environ['GCS_PREFIX']}_ceridian"),
        dag = dag
)

ceridian_gcs >> ceridian_dataflow >> ceridian_bq_load >> remove_non_employees >> \
create_gender_comp_table >> create_racial_comp_table >> ceridian_export >> beam_cleanup