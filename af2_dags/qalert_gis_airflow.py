from __future__ import absolute_import
import os

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_month, get_ds_year, default_args





# The goal of this DAG is to push a copy of the 311 all_linked_requests to data-bridGIS for the GIS team to build
# maps off the pipeline

dag = DAG(
    'qalert_gis',
    default_args=default_args,
    schedule_interval='@daily',
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year}
)


# create a table in data-bridGIS
push_table = BigQueryOperator(
        task_id = 'format_street_tix',
        sql = F"""
        CREATE OR REPLACE TABLE `data-bridgis.qalert.all_linked_requests` AS
        SELECT
            * EXCEPT (input_pii_long, input_pii_lat),
            ST_GEOGPOINT(input_pii_long, input_pii_lat) as geography
        FROM `{os.environ['GLCOUD_PROJECT']}.qalert.all_linked_requests`
        """,
        bigquery_conn_id='google_cloud_default',
        use_legacy_sql = False,
        dag = dag
)


beam_cleanup = BashOperator(
    task_id='beam_cleanup',
    bash_command= airflow_utils.beam_cleanup_statement('{}_qalert'.format(os.environ['GCS_PREFIX'])),
    dag=dag
)

push_table >> beam_cleanup