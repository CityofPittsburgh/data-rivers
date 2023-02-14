from __future__ import absolute_import

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_month, get_ds_year, get_ds_day, default_args, \
    build_insert_new_records_query, build_sync_update_query, build_dedup_old_updates

# This DAG will perform an hourly pull of incoming Cherwell Service Request and Incident tickets.
# Ticket information will be displayed on a Google Looker Studio dashboard for use by team managers
# that do not have access to Cherwell's admin portal.

dag = DAG(
    'cherwell_incidents',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2023, 2, 6),
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year,
                          'get_ds_day': get_ds_day}
)

# initialize gcs locations and store endpoint names in variables
source = "cherwell"
bucket = f"gs://{os.environ['GCS_PREFIX']}_{source}"
dataset = "incidents"
exec_date = "{{ ds }}"
path = "{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ run_id }}"
json_loc = f"{dataset}/{path}_{dataset}.json"
avro_loc = f"{dataset}/avro_output/{path}"
new_table = f"incoming_{dataset}"
master_table = f"all_{dataset}"
id_col = "id"
upd_fields = ['status', 'last_modified_date', 'closed_date', 'assigned_team', 'assigned_to', 'responded_date']

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

insert_new_incidents = BigQueryOperator(
        task_id='insert_new_incidents',
        sql=build_insert_new_records_query(source, new_table, master_table, id_col),
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
    sql=build_dedup_old_updates(source, master_table, id_col, 'last_modified_date'),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

beam_cleanup = BashOperator(
        task_id='beam_cleanup',
        bash_command=airflow_utils.beam_cleanup_statement(f"{os.environ['GCS_PREFIX']}_{source}"),
        dag=dag
)

cherwell_incidents_gcs >> cherwell_incidents_dataflow >> cherwell_incidents_bq_load >> insert_new_incidents >> \
    update_changed_incidents >> dedup_table >> beam_cleanup
