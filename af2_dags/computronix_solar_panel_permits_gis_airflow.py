from __future__ import absolute_import

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_bigquery import BigQueryToBigQueryOperator
from airflow.contrib.operators.bigquery_table_delete_operator import BigQueryTableDeleteOperator

from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_year, get_ds_month, get_ds_day, default_args, \
    build_geo_coords_from_parcel_query, build_revgeo_time_bound_query, check_blob_exists


# custom query builder func. to do all geo enrichment as a single query. this avoids unnecessary staging tables/views
# and has been tested as very efficient (as of 10/23)
# no args passed in here as this func is specific for this script and we are not currently
# (10/23) focused on code reusability. it is more concise to enter the table names/datasets directly. if this
# idea is extended to future dags, this should be converted to a utility func with the necessary arguments passed as
# vars
def build_geo_enrich_query():
    # geocode missing lat/long based on parcel number
    query_get_coords = build_geo_coords_from_parcel_query(
            F"{os.environ['GCLOUD_PROJECT']}.computronix.incoming_solar_panel_permits",
            "parc_num")

    # reverse geocode city geographic zones from lat/long
    query_geo_join = build_revgeo_time_bound_query('computronix', 'get_coords',
                                                   'completed_date_UTC', 'latitude', 'longitude',
                                                   source_is_table = False)

    # combine the two queries into a Create Table (the final table) and CTE query
    query_geo_enrich = F"""
    CREATE OR REPLACE TABLE `{os.environ['GCLOUD_PROJECT']}.computronix.geo_enriched_solar_panels` AS
    WITH get_coords AS 
    (
    {query_get_coords}
    )
    {query_geo_join}
    """
    return query_geo_enrich


dag = DAG(
        'computronix_solar_panel_gis',
        default_args = default_args,
        schedule_interval = '@daily',
        user_defined_filters = {'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year, 'get_ds_day': get_ds_day},
        start_date = datetime(2022, 10, 24),
        catchup = False
)

# initialize gcs/bq locations and vars
run_id_path = "{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ ds|get_ds_day }}/{{ run_id }}"

ingest_bucket = f"gs://{os.environ['GCS_PREFIX']}_computronix"
ingest_json_loc = f"{run_id_path}_solar_panel_permits.json"
transformation_output_hot_bucket = f"{os.environ['GCS_PREFIX']}_hot_metal"

dataset = "solar_panel_permits"

# Run gcs_loader
exec_gcs = F"python {os.environ['GCS_LOADER_PATH']}/computronix_electric_permits_gcs.py"
gcs_loader = BashOperator(
        task_id = 'gcs_loader',
        bash_command = F"{exec_gcs} --output_arg {dataset}/{ingest_json_loc}",
        dag = dag
)


# Short-circuit DAG if no backfill needs to be performed
check_api_retrieval_error = ShortCircuitOperator(
    task_id='check_api_retrieval_error',
    python_callable=check_blob_exists,
    op_kwargs={"bucket": F"{os.environ['GCS_PREFIX']}_computronix", "path": F"{dataset}/{ingest_json_loc}"},
    dag=dag
)


# Run dataflow
exec_df = F"python {os.environ['DATAFLOW_SCRIPT_PATH']}/computronix_solar_panel_permits_gis_dataflow.py"
dataflow = BashOperator(
        task_id = 'dataflow',
        bash_command = F"{exec_df} --input {ingest_bucket}/{dataset}/{ingest_json_loc} --avro_output"
                       F" gs://{transformation_output_hot_bucket}/solar_avro/",
        dag = dag
)

# Table size does not currently (10/23) warrant partitioning, but as data volume increases (expected around 2025) add
# table partitioning. Note: data truncated w/ each run so ingestion time partition is the wrong move. Consider using
# permit application or work completion date. Monthly or yearly (not daily) will be appropriate.
gcs_to_bq = GoogleCloudStorageToBigQueryOperator(
        task_id = 'gcs_to_bq',
        destination_project_dataset_table = F"{os.environ['GCLOUD_PROJECT']}:computronix.incoming_solar_panel_permits",
        bucket = F"{transformation_output_hot_bucket}",
        source_objects = ["solar_avro/*.avro"],
        write_disposition = 'WRITE_TRUNCATE',
        create_disposition = 'CREATE_IF_NEEDED',
        source_format = 'AVRO',
        autodetect = True,
        bigquery_conn_id = 'google_cloud_default',
        dag = dag
)

bq_geo_enrichment = BigQueryOperator(
        task_id = 'bq_geo_enrichment',
        sql = build_geo_enrich_query(),
        bigquery_conn_id = 'google_cloud_default',
        use_legacy_sql = False,
        dag = dag
)

exp_to_gis_bq = BigQueryToBigQueryOperator(
        task_id = 'exp_to_gis_bq',
        source_project_dataset_tables = F"{os.environ['GCLOUD_PROJECT']}:computronix.geo_enriched_solar_panels",
        destination_project_dataset_table = F"data-bridgis:computronix.geo_enriched_solar_panels",
        write_disposition = 'WRITE_TRUNCATE',
        create_disposition = 'CREATE_IF_NEEDED',
        bigquery_conn_id = 'google_cloud_default',
        dag = dag
)

del_hot_bucket_avro = BashOperator(
        task_id = 'del_hot_bucket_avro',
        bash_command = F"gsutil rm -r gs://{transformation_output_hot_bucket}/solar_avro/*.avro",
        dag = dag
)

# the incoming solar panels table is the first staging table in the initial load operation. with a write_disposition
# of truncate, the table is overwritten each dag run and doesn't technically need to be deleted. in practice,
# hanging on to staging tables has caused confusion around which tables should be used for analysis. thus,
# it is best to delete here. the load operation will still have a truncate disposition to ensure that the table is
# correctly overwritten in the event of a previous dag run failure (as of 10/23 this will be the SOP for new
# pipelines)
delete_incoming_table = BigQueryTableDeleteOperator(
        task_id = "delete_incoming_table",
        deletion_dataset_table = F"{os.environ['GCLOUD_PROJECT']}.computronix.incoming_solar_panel_permits",
)


beam_cleanup = BashOperator(
        task_id = 'beam_cleanup',
        bash_command = airflow_utils.beam_cleanup_statement(F"{os.environ['GCS_PREFIX']}_computronix"),
        dag = dag
)


gcs_loader >> check_api_retrieval_error >> dataflow >> gcs_to_bq
gcs_to_bq >> del_hot_bucket_avro
gcs_to_bq >> beam_cleanup
gcs_to_bq >> bq_geo_enrichment >> exp_to_gis_bq >> delete_incoming_table


