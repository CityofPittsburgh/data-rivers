from __future__ import absolute_import

import os
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.operators.python_operator import PythonOperator
from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_month, get_ds_year, get_ds_day, default_args, perform_data_quality_check

from dependencies.bq_queries.employee_admin import intime_admin as i_q
from dependencies.bq_queries import general_queries as g_q

# The goal of this DAG is to perform a complete pull of police rank assignment data from
# the InTime API. This employee info will be stored in Data Rivers and extracted via PowerShell
# to be merged into the Police Active Directory.


COLS_IN_ORDER = """assignment_id, employee_id, display_name, sub_assignment, parent_assignment_id, permanent_rank, unit, 
court_assignment, location_group, sub_location_name, activity_name, assignment_date, scheduled_start_time, 
scheduled_end_time, actual_start_time, actual_end_time, hours_modifier_name, hours_modifier_type, hours_sched_min_hours,
time_bank_name, time_bank_type, time_bank_hours"""

dag = DAG(
    'intime_assignments',
    default_args=default_args,
    schedule_interval='@hourly',
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year,
                          'get_ds_day': get_ds_day},
    max_active_runs=1
)

# initialize gcs locations
dataset = "intime"
json_bucket = f"gs://{os.environ['GCS_PREFIX']}_{dataset}"
hot_bucket = f"gs://{os.environ['GCS_PREFIX']}_hot_metal"
path = "assignments/{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ ds|get_ds_day }}/{{ run_id }}"
json_loc = f"{path}_assignments.json"
output_name = "schedule_assignments"
dq_checker = 'intime_sub_locations'
date_fields = [{'field': 'scheduled_start_time', 'type': 'TIMESTAMP'},
               {'field': 'scheduled_end_time', 'type': 'TIMESTAMP'},
               {'field': 'actual_start_time', 'type': 'TIMESTAMP'},
               {'field': 'actual_end_time', 'type': 'TIMESTAMP'}]

intime_assignments_gcs = BashOperator(
    task_id='intime_assignments_gcs',
    bash_command=f"python {os.environ['GCS_LOADER_PATH']}/intime_assignments_gcs.py --output_arg {json_loc}",
    execution_timeout=timedelta(hours=1),
    dag=dag
)

intime_assignments_dataflow = BashOperator(
    task_id='intime_assignments_dataflow',
    bash_command=f"python {os.environ['DATAFLOW_SCRIPT_PATH']}/intime_assignments_dataflow.py "
                 f"--input {json_bucket}/{json_loc} "
                 f"--avro_output {hot_bucket}/{output_name}",
    dag=dag
)

intime_assignments_bq_load = GoogleCloudStorageToBigQueryOperator(
    task_id='intime_assignments_bq_load',
    destination_project_dataset_table=f"{os.environ['GCLOUD_PROJECT']}.{dataset}.incoming_assignments",
    bucket=f"{os.environ['GCS_PREFIX']}_hot_metal",
    source_objects=[f"{output_name}*.avro"],
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    source_format='AVRO',
    autodetect=True,
    bigquery_conn_id='google_cloud_default',
    dag=dag
)

format_data_types_query = g_q.build_format_dedup_query(dataset, 'incoming_assignments', 'incoming_assignments',
                                                       date_fields, COLS_IN_ORDER,
                                                       datestring_fmt="%Y-%m-%d %H:%M:%S %Z", tz="America/New_York")
format_data_types = BigQueryOperator(
    task_id='format_data_types',
    sql=format_data_types_query,
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

write_append_query = F"""
CREATE OR REPLACE TABLE `{os.environ['GCLOUD_PROJECT']}.{dataset}.schedule_assignments` AS
    SELECT DISTINCT * FROM `{os.environ['GCLOUD_PROJECT']}.{dataset}.incoming_assignments`
    UNION DISTINCT
    SELECT DISTINCT * FROM `{os.environ['GCLOUD_PROJECT']}.{dataset}.{output_name}`
"""
write_append_data = BigQueryOperator(
    task_id='write_append_data',
    sql=write_append_query,
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

create_data_quality_table = BigQueryOperator(
    task_id='create_data_quality_table',
    sql=g_q.build_data_quality_table(dataset, dq_checker, 'schedule_assignments', 'sub_location_name'),
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

# union all up-to-date assignment info with the permanent employee information taken from InTime
merge_intime_data = BigQueryOperator(
    task_id='merge_intime_data',
    sql=i_q.extract_current_intime_details(),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

# Export merged table to IAPro bucket as readable CSV
assignment_iapro_export = BigQueryToCloudStorageOperator(
    task_id='assignment_iapro_export',
    source_project_dataset_table=f"{os.environ['GCLOUD_PROJECT']}.{dataset}.pbp_current_assignments",
    destination_cloud_storage_uris=[f"gs://{os.environ['GCS_PREFIX']}_iapro/current_assignments_report.csv"],
    bigquery_conn_id='google_cloud_default',
    dag=dag
)

beam_cleanup = BashOperator(
    task_id='beam_cleanup',
    bash_command=airflow_utils.beam_cleanup_statement(f"{os.environ['GCS_PREFIX']}_{dataset}"),
    dag=dag
)

intime_assignments_gcs >> intime_assignments_dataflow >> intime_assignments_bq_load >> format_data_types >> \
    write_append_data >> create_data_quality_table >> export_data_quality >> data_quality_check >> beam_cleanup
intime_assignments_gcs >> intime_assignments_dataflow >> intime_assignments_bq_load >> format_data_types >> \
    merge_intime_data >> assignment_iapro_export >> beam_cleanup
