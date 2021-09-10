from __future__ import absolute_import

import os

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator

from dependencies import airflow_utils
from dependencies.airflow_utils import build_revgeo_query, get_ds_year, get_ds_month, default_args, \
    format_gcs_call, format_dataflow_call, build_city_limits_query

# TODO: When Airflow 2.0 is released, upgrade the package, sub in DataFlowPythonOperator for BashOperator,
#  and pass the argument 'py_interpreter=python3'

PII_FIELDS = """pii_comments, pii_google_formatted_address, pii_input_address, pii_lat, pii_long, pii_private_notes, 
pii_street_num"""

dag = DAG(
        'qalert_requests',
        default_args = default_args,
        schedule_interval = '@daily',
        user_defined_filters = {'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year}
)

# Run gcs_loader
qalert_requests_gcs = BashOperator(
        task_id = 'qalert_gcs',
        bash_command = format_gcs_call("qalert_gcs.py", f"{os.environ['GCS_PREFIX']}_qalert",
                                       "requests"),
        dag = dag
)

# Run dataflow_script
qalert_requests_dataflow = BashOperator(
        task_id = 'qalert_dataflow',
        bash_command = format_dataflow_call("qalert_requests_dataflow.py"),
        dag = dag
)

# Load AVRO data produced by dataflow_script into BQ temp table
qalert_requests_bq = GoogleCloudStorageToBigQueryOperator(
        task_id = 'qalert_bq',
        destination_project_dataset_table = '{}:qalert.temp_new_req'.format(os.environ['GCLOUD_PROJECT']),
        bucket = f"{os.environ['GCS_PREFIX']}_qalert",
        source_objects = ["requests/avro_output/{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ ds }}/*.avro"],
        write_disposition = 'WRITE_TRUNCATE',
        create_disposition = 'CREATE_IF_NEEDED',
        source_format = 'AVRO',
        autodetect = True,
        time_partitioning = {'type': 'DAY'},
        dag = dag
)

<<<<<<< HEAD
# Query new tickets (temp table ^^) to determine if they are in the city limits
=======
# TODO: update with lat/long cast as floats
conv_query = ("CREATE OR REPLACE TABLE `{}:qalert.temp_new_req`".format(os.environ['GCLOUD_PROJECT']) +
              " AS SELECT DISTINCT * EXCEPT (pii_lat, pii_long, anon_lat, anon_long), " +
              "   CAST(pii_lat AS FLOAT64) AS pii_lat, "
              "   CAST(pii_long AS FLOAT64) AS pii_long, "
              "   CAST(anon_lat AS FLOAT64) AS anon_lat, "
              "   CAST(anon_long AS FLOAT64) AS anon_long, "
              "FROM `{}:qalert.temp_new_req`".format(os.environ['GCLOUD_PROJECT']))

qalert_requests_coord_float_conv = BigQueryOperator(
        task_id='qalert_coord_float_conv',
        sql=conv_query,
        use_legacy_sql=False,
        destination_dataset_table='{}:qalert.temp_new_req'.format(os.environ['GCLOUD_PROJECT']),
        write_disposition='WRITE_TRUNCATE',
        create_disposition = 'CREATE_IF_NEEDED',
        time_partitioning={'type': 'DAY'},
        dag=dag
)

# TODO: de dupe prior to first push to new_parent and new_child tix?

# 2) query new tickets to determine if they are in the city limits
>>>>>>> 4b8981440d9dec5e4455d1b316095f33c3fc477d
qalert_requests_city_limits = BigQueryOperator(
        task_id = 'qalert_city_limits',
        sql = build_city_limits_query('qalert', 'temp_new_req', 'id'),
        use_legacy_sql = False,
        destination_dataset_table = f"{os.environ['GCLOUD_PROJECT']}:qalert.temp_new_req",
        write_disposition = 'WRITE_APPEND',
        time_partitioning = {'type': 'DAY'},
        dag = dag
)

# Join all the geo information (e.g. DPW districts, etc) to the new data
qalert_requests_geojoin = BigQueryOperator(
        task_id = 'qalert_geojoin',
        sql = build_revgeo_query('qalert', 'temp_new_req', 'id', 'pii_lat', 'pii_long'),
        use_legacy_sql = False,
        destination_dataset_table = f"{os.environ['GCLOUD_PROJECT']}:qalert.temp_new_req",
        write_disposition = 'WRITE_APPEND',
        time_partitioning = {'type': 'DAY'},
        dag = dag
)

#  Append the geojoined to all_requests
append_query = f"""
                INSERT INTO `{os.environ['GCLOUD_PROJECT']}:qalert.all_requests`
                SELECT * FROM `{os.environ['GCLOUD_PROJECT']}:qalert.temp_new_req`
"""

qalert_requests_merge_new_tickets = BigQueryOperator(
        task_id = 'qalert_merge_new_tickets',
        sql = append_query,
        use_legacy_sql = False,
        destination_dataset_table = f"{os.environ['GCLOUD_PROJECT']}:qalert.all_requests",
        write_disposition = 'WRITE_APPEND',
        create_disposition = 'CREATE_IF_NEEDED',
        time_partitioning = {'type': 'DAY'},
        dag = dag
)


# Create a table from all_linked_requests that has all columns EXCEPT those that have potential PII. This table is
# subsequently exported to WPRDC
drop_pii_query = f"""
SELECT * except ({PII_FIELDS})  
FROM `{os.environ["GCLOUD_PROJECT"]}.qalert.all_linked_requests`
"""

qalert_requests_drop_pii_for_export = BigQueryOperator(
        task_id = 'qalert_requests_drop_pii_for_export',
        sql = drop_pii_query,
        use_legacy_sql = False,
        destination_dataset_table = f"{os.environ['GCLOUD_PROJECT']}:qalert.wprdc_export",
        write_disposition = 'WRITE_TRUNCATE',
        create_disposition = 'CREATE_IF_NEEDED',
        time_partitioning = {'type': 'DAY'},
        dag = dag
)

# Export table as CSV to WPRDC bucket
qalert_wprdc_export = BigQueryToCloudStorageOperator(
        task_id = 'qalert_wprdc_export',
        source_project_dataset_table = f"{os.environ['GCLOUD_PROJECT']}:qalert.wprdc_export",
        destination_cloud_storage_uris = f"gs://{os.environ['GCS_PREFIX']}_wprdc/qalert_permits_{{ ds }}.csv",
        dag = dag
)

# Drop all temporary tables
qalert_bq_drop_temp = BigQueryOperator(
        task_id = 'qalert_bq_drop_temp',
        sql = f"DROP TABLE (`{os.environ['GCLOUD_PROJECT']}.qalert.temp_new_req`, {os.environ['GCLOUD_PROJECT']}:qalert.wprdc_export)",
        use_legacy_sql = False,
        dag = dag
)

# Clean up
qalert_beam_cleanup = BashOperator(
        task_id = 'qalert_beam_cleanup',
        bash_command = airflow_utils.beam_cleanup_statement(f"{os.environ['GCS_PREFIX']}_qalert"),
        dag = dag
)

qalert_requests_gcs >> qalert_requests_dataflow >> qalert_requests_bq >> qalert_requests_city_limits >> \
qalert_requests_geojoin >> qalert_requests_merge_new_tickets >> qalert_requests_split_parent >> \
qalert_requests_split_child >> qalert_merge_related_requests >> qalert_bq_drop_temp >> qalert_beam_cleanup
