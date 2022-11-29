from __future__ import absolute_import

import os

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator

from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_year, get_ds_month, get_ds_day, default_args


# STEPS TO TAKE BEFORE EXECUTING DAG:
# 1.) Create backfill folder within cartegraph/tasks/backfill GCS bucket
# 2.) Create successful_run_log sub-folder within the directory created in step 1
# 3.) Make backups of all_tasks in case something goes wrong

# This DAG schedule interval set to None because it will only ever be triggered manually
dag = DAG(
    'cartegraph_backfill_activities',
    default_args=default_args,
    schedule_interval=None,
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year,
                          'get_ds_day': get_ds_day}
)

path = "{{ ds|get_ds_year }}-{{ ds|get_ds_month }}-{{ ds|get_ds_day }}"

gcs_loader = BashOperator(
    task_id='gcs_loader',
    bash_command=F"python {os.environ['DAGS_PATH']}/dependencies/gcs_loaders/cartegraph_tasks_activity_backfill_gcs.py "
                 F"--output_arg tasks/backfill/{path}/backfilled_activity_tasks.json",
    dag=dag
)

# Run dataflow_script
py_cmd = f"python {os.environ['DAGS_PATH']}/dependencies/dataflow_scripts/cartegraph_tasks_activity_backfill_dataflow.py"
in_cmd = \
    f" --input gs://{os.environ['GCS_PREFIX']}_cartegraph/tasks/backfill/{path}/backfilled_activity_tasks.json"
out_cmd = f" --avro_output gs://{os.environ['GCS_PREFIX']}_cartegraph/tasks/backfill/{path}/avro_output/"
df_cmd_str = py_cmd + in_cmd + out_cmd
dataflow = BashOperator(
    task_id='dataflow',
    bash_command=df_cmd_str,
    dag=dag
)

# Load AVRO data produced by dataflow_script into BQ temp table
gcs_to_bq = GoogleCloudStorageToBigQueryOperator(
    task_id='gcs_to_bq',
    bigquery_conn_id='google_cloud_default',
    destination_project_dataset_table=f"{os.environ['GCLOUD_PROJECT']}:backfill.all_activities",
    bucket=f"{os.environ['GCS_PREFIX']}_cartegraph",
    source_objects=[f"tasks/backfill/{path}/avro_output/*.avro"],
    write_disposition='WRITE_APPEND',
    create_disposition='CREATE_IF_NEEDED',
    source_format='AVRO',
    autodetect=True,
    dag=dag
)

query_join_tables = f"""
CREATE OR REPLACE TABLE `{os.environ['GCLOUD_PROJECT']}.cartegraph.all_tasks` AS
SELECT 
    tsk.id, act.activity, department, status, entry_date_UTC, entry_date_EST, entry_date_UNIX, 
    actual_start_date_UTC, actual_start_date_EST, actual_start_date_UNIX, actual_stop_date_UTC, actual_stop_date_EST, 
    actual_stop_date_UNIX, labor_cost, equipment_cost, material_cost, labor_hours, request_issue, request_department, 
    request_location, asset_id, asset_type, task_description, task_notes, neighborhood_name, council_district, ward, 
    police_zone, fire_zone, dpw_streets, dpw_enviro, dpw_parks, lat, long
FROM `{os.environ['GCLOUD_PROJECT']}.cartegraph.all_tasks` tsk
JOIN `{os.environ['GCLOUD_PROJECT']}.backfill.all_activities` act
ON tsk.id = act.id;
"""
join_activities = BigQueryOperator(
    task_id='join_activities',
    sql=query_join_tables,
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

# Clean up
beam_cleanup = BashOperator(
    task_id='cartegraph_beam_cleanup',
    bash_command=airflow_utils.beam_cleanup_statement(f"{os.environ['GCS_PREFIX']}_cartegraph"),
    dag=dag
)

# DAG execution:
gcs_loader >> dataflow >> gcs_to_bq >> join_activities >> beam_cleanup
