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

# The goal of this DAG is to perform a daily pull of basic demographic information for each
# City of Pittsburgh employee via the Ceridian Dayforce API. This  data will be stored securely
# in Data Rivers and used for a few projects. For one, we will use the list of employees returned
# by the API to determine who has left the City payroll so that we can stop keeping track of
# their COVID vaccination status. Additionally, we will use de-identified race, sex, and union
# membership totals to display on Dashburgh. This will give the public insight on the demographics
# of the city government and how it compares to the demographics of the city as a whole.

SAFE_FIELDS = """employee_num, first_name, last_name, display_name, sso_login, dept_desc, office, job_title, 
hire_date, termination_date, account_modified_date, `union`, status, pay_class, manager_name, ethnicity, gender"""

dag = DAG(
    'ceridian_employees',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2023, 2, 8),
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
dq_checker = "ceridian_departments"
date_fields = [{'field': 'hire_date', 'type': 'DATE'},
               {'field': 'termination_date', 'type': 'DATE'},
               {'field': 'account_modified_date', 'type': 'DATE'}]

ceridian_employees_gcs = BashOperator(
    task_id='ceridian_employees_gcs',
    bash_command=f"python {os.environ['GCS_LOADER_PATH']}/ceridian_employees_gcs.py --output_arg {json_loc}",
    dag=dag
)

ceridian_employees_dataflow = BashOperator(
    task_id='ceridian_employees_dataflow',
    bash_command=f"python {os.environ['DATAFLOW_SCRIPT_PATH']}/ceridian_employees_dataflow.py "
                 f"--input {bucket}/{json_loc} --avro_output {bucket}/{avro_loc}",
    dag=dag
)

ceridian_employees_bq_load = GoogleCloudStorageToBigQueryOperator(
    task_id='ceridian_employees_bq_load',
    destination_project_dataset_table=f"{os.environ['GCLOUD_PROJECT']}.ceridian.all_employees",
    bucket=f"{os.environ['GCS_PREFIX']}_ceridian",
    source_objects=[f"{avro_loc}*.avro"],
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    source_format='AVRO',
    autodetect=True,
    bigquery_conn_id='google_cloud_default',
    dag=dag
)

create_data_quality_table = BigQueryOperator(
    task_id='create_data_quality_table',
    sql=g_q.build_data_quality_table('ceridian', dq_checker, 'all_employees', 'dept_desc'),
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

gender_table = 'employee_vs_gen_pop_gender_comp'
gender_pct_field = 'gender'
gender_hardcoded_vals = [{gender_pct_field: 'M', 'percentage': 00.49},
                         {gender_pct_field: 'F', 'percentage': 00.51}]
create_gender_comp_table = BigQueryOperator(
    task_id='create_gender_comp_table',
    sql=c_q.build_percentage_table_query(gender_table, gender_pct_field, gender_hardcoded_vals),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

race_table = 'employee_vs_gen_pop_racial_comp'
race_pct_field = 'ethnicity'
race_hardcoded_vals = [{race_pct_field: 'White (not Hispanic or Latino)', 'percentage': 00.645},
                       {race_pct_field: 'Black or African American (not Hispanic or Latino)', 'percentage': 00.23},
                       {race_pct_field: 'Asian (not Hispanic or Latino)', 'percentage': 00.058},
                       {race_pct_field: 'Hispanic or Latino', 'percentage': 00.034},
                       {race_pct_field: 'American Indian or Alaska Native (not Hispanic or Latino)',
                        'percentage': 00.002},
                       {race_pct_field: 'Native Hawaiian or Other Pacific Islander (not Hispanic or Latino)',
                        'percentage': 00.001},
                       {race_pct_field: 'Two or More Races  (not Hispanic or Latino)', 'percentage': 00.036}]
create_racial_comp_table = BigQueryOperator(
    task_id='create_racial_comp_table',
    sql=c_q.build_percentage_table_query(race_table, race_pct_field, race_hardcoded_vals),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

create_recent_terminations_table = BigQueryOperator(
    task_id='create_recent_terminations_table',
    sql=c_q.extract_recent_terminations(),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

recent_terminations_to_gcs = BigQueryToCloudStorageOperator(
    task_id='recent_terminations_to_gcs',
    source_project_dataset_table=f"{os.environ['GCLOUD_PROJECT']}.ceridian.past_month_terminations",
    destination_cloud_storage_uris=[f"gs://{os.environ['GCS_PREFIX']}_iapro/past_month_terminations.csv"],
    bigquery_conn_id='google_cloud_default',
    dag=dag
)

create_pmo_export_table = BigQueryOperator(
    task_id='create_pmo_export_table',
    sql=c_q.pmo_export_query(),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

ceridian_pmo_export = BigQueryToCloudStorageOperator(
    task_id='ceridian_pmo_export',
    source_project_dataset_table=f"{os.environ['GCLOUD_PROJECT']}.ceridian.active_non_ps_employees",
    destination_cloud_storage_uris=[f"gs://{os.environ['GCS_PREFIX']}_pmo/training/active_non_ps_employees.csv"],
    bigquery_conn_id='google_cloud_default',
    dag=dag
)

create_iapro_export_table = BigQueryOperator(
    task_id='create_iapro_export_table',
    sql=g_q.build_format_dedup_query('ceridian', 'ceridian_ad_export', 'all_employees', date_fields, SAFE_FIELDS,
                                     datestring_fmt="%Y-%m-%d", tz="America/New_York"),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

ceridian_iapro_export = BigQueryToCloudStorageOperator(
    task_id='ceridian_iapro_export',
    source_project_dataset_table=f"{os.environ['GCLOUD_PROJECT']}.ceridian.ceridian_ad_export",
    destination_cloud_storage_uris=[f"gs://{os.environ['GCS_PREFIX']}_iapro/ceridian_report.csv"],
    bigquery_conn_id='google_cloud_default',
    dag=dag
)

beam_cleanup = BashOperator(
    task_id='ceridian_beam_cleanup',
    bash_command=airflow_utils.beam_cleanup_statement(f"{os.environ['GCS_PREFIX']}_ceridian"),
    dag=dag
)

ceridian_employees_gcs >> ceridian_employees_dataflow >> ceridian_employees_bq_load >> create_data_quality_table >> \
    export_data_quality >> data_quality_check >> beam_cleanup
ceridian_employees_gcs >> ceridian_employees_dataflow >> ceridian_employees_bq_load >> create_iapro_export_table >> \
    ceridian_iapro_export >> beam_cleanup
ceridian_employees_gcs >> ceridian_employees_dataflow >> ceridian_employees_bq_load >> \
    create_recent_terminations_table >> recent_terminations_to_gcs >> beam_cleanup
ceridian_employees_gcs >> ceridian_employees_dataflow >> ceridian_employees_bq_load >> create_pmo_export_table >> \
    ceridian_pmo_export >> beam_cleanup
ceridian_employees_gcs >> ceridian_employees_dataflow >> ceridian_employees_bq_load >> create_gender_comp_table >> \
    create_racial_comp_table >> beam_cleanup
