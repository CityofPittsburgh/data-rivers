from __future__ import absolute_import

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_month, get_ds_year, get_ds_day, default_args, \
    build_insert_new_records_query, build_sync_update_query, build_dedup_old_updates, build_format_dedup_query

# This DAG will perform a hourly pull of incoming Cherwell Service Request and Incident tickets.
# Ticket information will be displayed on a Google Looker Studio dashboard for use by team managers
# that do not have access to Cherwell's admin portal.

COLS_IN_ORDER = """id, created_date_EST, created_date_UTC, created_date_UNIX, status, service, category, subcategory, 
description, priority, last_modified_date_EST, last_modified_date_UTC, last_modified_date_UNIX, closed_date_EST, 
closed_date_UTC, closed_date_UNIX, assigned_team, assigned_to, assigned_to_manager, incident_type, 
respond_by_deadline_EST, respond_by_deadline_UTC, respond_by_deadline_UNIX, resolve_by_deadline_EST, 
resolve_by_deadline_UTC, resolve_by_deadline_UNIX, call_source, incident_reopened, responded_date_EST, 
responded_date_UTC, responded_date_UNIX, resolved_date_EST, resolved_date_UTC, resolved_date_UNIX, number_of_touches,
number_of_escalations, requester_department, requester, on_behalf_of,
initial_assigned_team, """

dag = DAG(
    'cherwell_incidents',
    default_args=default_args,
    schedule_interval='@hourly',
    start_date=datetime(2023, 4, 19),
    catchup=False,
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year,
                          'get_ds_day': get_ds_day},
    max_active_runs=1
)

# initialize gcs locations and store endpoint names in variables
source = "cherwell"
bucket = f"gs://{os.environ['GCS_PREFIX']}_{source}"
dataset = "incidents"
exec_date = "{{ ds }}"
path = "{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ ds|get_ds_day }}/{{ run_id }}"
json_loc = f"{dataset}/{path}_{dataset}.json"
avro_loc = f"{dataset}/avro_output/{path}"
new_table = f"incoming_{dataset}"
master_table = f"all_{dataset}"
id_col = "id"
date_fields = [{'field': 'created_date_EST', 'type': 'DATETIME'},
               {'field': 'created_date_UTC', 'type': 'DATETIME'},
               {'field': 'last_modified_date_EST', 'type': 'DATETIME'},
               {'field': 'last_modified_date_UTC', 'type': 'DATETIME'},
               {'field': 'closed_date_EST', 'type': 'DATETIME'},
               {'field': 'closed_date_UTC', 'type': 'DATETIME'},
               {'field': 'respond_by_deadline_EST', 'type': 'DATETIME'},
               {'field': 'respond_by_deadline_UTC', 'type': 'DATETIME'},
               {'field': 'resolve_by_deadline_EST', 'type': 'DATETIME'},
               {'field': 'resolve_by_deadline_UTC', 'type': 'DATETIME'},
               {'field': 'responded_date_EST', 'type': 'DATETIME'},
               {'field': 'responded_date_UTC', 'type': 'DATETIME'},
               {'field': 'resolved_date_EST', 'type': 'DATETIME'},
               {'field': 'resolved_date_UTC', 'type': 'DATETIME'}]
upd_fields = ['status', 'priority', 'last_modified_date_EST', 'last_modified_date_UTC', 'last_modified_date_UNIX',
              'closed_date_EST', 'closed_date_UTC', 'closed_date_UNIX', 'assigned_team', 'assigned_to',
              'assigned_to_manager', 'responded_date_EST', 'responded_date_UTC', 'responded_date_UNIX',
              'resolved_date_EST', 'resolved_date_UTC', 'resolved_date_UNIX', 'respond_by_deadline_EST',
              'respond_by_deadline_UTC', 'respond_by_deadline_UNIX', 'resolve_by_deadline_EST',
              'resolve_by_deadline_UTC', 'resolve_by_deadline_UNIX', 'number_of_touches', 'number_of_escalations',
              'requester_department', 'requester', 'incident_reopened']

cherwell_incidents_gcs = BashOperator(
    task_id='cherwell_incidents_gcs',
    bash_command=f"python {os.environ['GCS_LOADER_PATH']}/cherwell_incidents_gcs.py --output_arg {json_loc}",
    dag=dag
)

cherwell_incidents_dataflow = BashOperator(
    task_id='cherwell_incidents_dataflow',
    bash_command=f"python {os.environ['DATAFLOW_SCRIPT_PATH']}/cherwell_incidents_dataflow.py "
                 f"--input {bucket}/{json_loc} --avro_output {bucket}/{avro_loc}",
    dag=dag
)

cherwell_incidents_bq_load = GoogleCloudStorageToBigQueryOperator(
    task_id='cherwell_incidents_bq_load',
    destination_project_dataset_table=f"{os.environ['GCLOUD_PROJECT']}.{source}.{new_table}",
    bucket=f"{os.environ['GCS_PREFIX']}_{source}",
    source_objects=[f"{avro_loc}*.avro"],
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    source_format='AVRO',
    autodetect=True,
    bigquery_conn_id='google_cloud_default',
    dag=dag
)

format_data_types_query = build_format_dedup_query(source, new_table, new_table, date_fields,
                                                   # Preserve name of person initially assigned to ticket
                                                   f"{COLS_IN_ORDER} assigned_to AS initial_assigned_to",
                                                   datestring_fmt="%m/%d/%Y %I:%M:%S %p")
format_data_types = BigQueryOperator(
    task_id='format_data_types',
    sql=format_data_types_query,
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

insert_new_incidents = BigQueryOperator(
    task_id='insert_new_incidents',
    sql=build_insert_new_records_query(source, new_table, master_table, id_col,
                                       f"{COLS_IN_ORDER} initial_assigned_to"),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

update_changed_incidents = BigQueryOperator(
    task_id='update_changed_incidents',
    sql=build_sync_update_query(source, master_table, new_table, id_col, upd_fields),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

dedup_table = BigQueryOperator(
    task_id='dedup_table',
    sql=build_dedup_old_updates(source, master_table, id_col, 'last_modified_date_UNIX'),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

beam_cleanup = BashOperator(
    task_id='beam_cleanup',
    bash_command=airflow_utils.beam_cleanup_statement(f"{os.environ['GCS_PREFIX']}_{source}"),
    dag=dag
)

cherwell_incidents_gcs >> cherwell_incidents_dataflow >> cherwell_incidents_bq_load >> format_data_types >> \
    insert_new_incidents >> update_changed_incidents >> dedup_table >> beam_cleanup
