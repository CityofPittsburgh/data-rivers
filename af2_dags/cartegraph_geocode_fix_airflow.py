from __future__ import absolute_import

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_month, get_ds_year, default_args, \
    build_revgeo_view_query, build_dedup_old_updates

COLS_IN_ORDER = """id, activity, department, status, entry_date_UTC, entry_date_EST, entry_date_UNIX, 
actual_start_date_UTC, actual_start_date_EST, actual_start_date_UNIX, actual_stop_date_UTC, actual_stop_date_EST, 
actual_stop_date_UNIX, labor_cost, equipment_cost, material_cost, labor_hours, request_issue, request_department, 
request_location, asset_id, asset_type, task_description, task_notes, neighborhood_name, council_district, ward, 
police_zone, fire_zone, dpw_streets, dpw_enviro, dpw_parks, lat, long"""

# The goal of this mini-DAG is to assign geographic zone to Cartegraph task records that were not geocoded in the main
# Cartegraph Tasks DAG because they had null values for their actual_start_date field. While actual_start_date gives a
# better idea about the time an actual task occurred (for example, an administrator could enter a task that was
# completed in a decade ago in 2022 - just because the entry_date field is in 2022 doesn't mean that's when the task
# actually happened). A limitation to the approach of using actual_start_date as the date field for assigning geographic
# zones is that sometimes administrators leave this field blank, while entry_date is never blank. This DAG geocodes

dag = DAG(
    'cartegraph_task_geocode_fix',
    default_args=default_args,
    schedule_interval='@monthly',
    start_date=datetime(2022, 12, 1),
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year}
)

def init_cmds_xcomm(**kwargs):
    dataset = 'cartegraph'
    raw_table = 'all_tasks'
    new_table = 'temp_task_geojoin'
    create_date = 'entry_date_UTC'
    id_col = 'id'
    lat_field = 'lat'
    long_field = 'long'
    geo_config = [{'geo_table': 'neighborhoods', 'geo_field': 'neighborhood_name'},
                  {'geo_table': 'council_districts', 'geo_field': 'council_district'},
                  {'geo_table': 'wards', 'geo_field': 'ward'},
                  {'geo_table': 'fire_zones', 'geo_field': 'fire_zone'},
                  {'geo_table': 'police_zones', 'geo_field': 'police_zone'},
                  {'geo_table': 'dpw_streets_divisions', 'geo_field': 'dpw_streets'},
                  {'geo_table': 'dpw_es_divisions', 'geo_field': 'dpw_enviro'},
                  {'geo_table': 'dpw_parks_divisions', 'geo_field': 'dpw_parks'}]

    init_table_query = F"""
    CREATE OR REPLACE TABLE `{os.environ["GCLOUD_PROJECT"]}.{dataset}.{new_table}` AS 
    SELECT * FROM `{os.environ["GCLOUD_PROJECT"]}.{dataset}.{raw_table}`
    WHERE lat IS NOT NULL AND long IS NOT NULL 
    """
    for dict in geo_config:
        init_table_query += f"AND {dict['geo_field']} IS NULL "
        kwargs['ti'].xcom_push(key = f"build_geo_view_{dict['geo_table']}", value = build_revgeo_view_query(
            dataset, new_table, f"merge_{dict['geo_table']}", create_date, id_col, lat_field, long_field,
            dict['geo_table'], dict['geo_field']))
    kwargs['ti'].xcom_push(key = "init_table_query", value = init_table_query)


push_xcom = PythonOperator(
        task_id='push_xcom',
        python_callable=init_cmds_xcomm,
        dag=dag
)

init_temp_geo_table = BigQueryOperator(
    task_id='init_temp_geo_table',
    sql=str("{{ ti.xcom_pull(task_ids='push_xcom', key='init_table_query') }}"),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

build_geo_view_neighborhoods = BigQueryOperator(
    task_id='build_geo_view_neighborhoods',
    sql=str("{{ ti.xcom_pull(task_ids='push_xcom', key='build_geo_view_neighborhoods') }}"),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

build_geo_view_council = BigQueryOperator(
    task_id='build_geo_view_council',
    sql=str("{{ ti.xcom_pull(task_ids='push_xcom', key='build_geo_view_council_districts') }}"),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

build_geo_view_ward = BigQueryOperator(
    task_id='build_geo_view_ward',
    sql=str("{{ ti.xcom_pull(task_ids='push_xcom', key='build_geo_view_wards') }}"),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

build_geo_view_fire = BigQueryOperator(
    task_id='build_geo_view_fire',
    sql=str("{{ ti.xcom_pull(task_ids='push_xcom', key='build_geo_view_fire_zones') }}"),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

build_geo_view_police = BigQueryOperator(
    task_id='build_geo_view_police',
    sql=str("{{ ti.xcom_pull(task_ids='push_xcom', key='build_geo_view_police_zones') }}"),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

build_geo_view_streets = BigQueryOperator(
    task_id='build_geo_view_streets',
    sql=str("{{ ti.xcom_pull(task_ids='push_xcom', key='build_geo_view_dpw_streets_divisions') }}"),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

build_geo_view_env = BigQueryOperator(
    task_id='build_geo_view_env',
    sql=str("{{ ti.xcom_pull(task_ids='push_xcom', key='build_geo_view_dpw_es_divisions') }}"),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

build_geo_view_parks = BigQueryOperator(
    task_id='build_geo_view_parks',
    sql=str("{{ ti.xcom_pull(task_ids='push_xcom', key='build_geo_view_dpw_parks_divisions') }}"),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

push_xcom >> init_temp_geo_table >> build_geo_view_neighborhoods
push_xcom >> init_temp_geo_table >> build_geo_view_council
push_xcom >> init_temp_geo_table >> build_geo_view_ward
push_xcom >> init_temp_geo_table >> build_geo_view_fire
push_xcom >> init_temp_geo_table >> build_geo_view_police
push_xcom >> init_temp_geo_table >> build_geo_view_streets
push_xcom >> init_temp_geo_table >> build_geo_view_env
push_xcom >> init_temp_geo_table >> build_geo_view_parks
