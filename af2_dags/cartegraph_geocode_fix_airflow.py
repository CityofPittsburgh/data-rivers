from __future__ import absolute_import

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_month, get_ds_year, default_args, \
    build_revgeo_view_query, upd_table_from_view

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
    kwargs['ti'].xcom_push(key="init_table_query", value=init_table_query)
    for dict in geo_config:
        init_table_query += f"AND {dict['geo_field']} IS NULL "
        revgeo_view_query = build_revgeo_view_query(dataset, new_table, f"merge_{dict['geo_table']}", create_date,
                                                    id_col, lat_field, long_field, dict['geo_table'], dict['geo_field'])
        kwargs['ti'].xcom_push(key=f"build_geo_view_{dict['geo_table']}", value=revgeo_view_query)
        geo_upd_query = upd_table_from_view(dataset, raw_table, f"merge_{dict['geo_table']}", id_col, dict['geo_field'])
        kwargs['ti'].xcom_push(key=f"upd_geo_field_{dict['geo_field']}", value=geo_upd_query)
        kwargs['ti'].xcom_push(key=f"del_geo_view_{dict['geo_table']}", value=f"""
                               bq rm -f -t `{os.environ['GCLOUD_PROJECT']}.{dataset}.merge_{dict['geo_table']}""")


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

build_geo_view_ngh = BigQueryOperator(
    task_id='build_geo_view_ngh',
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

upd_geo_field_ngh = BigQueryOperator(
    task_id='upd_geo_field_ngh',
    sql=str("{{ ti.xcom_pull(task_ids='push_xcom', key='upd_geo_field_neighborhood_name') }}"),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

upd_geo_field_council = BigQueryOperator(
    task_id='upd_geo_field_council',
    sql=str("{{ ti.xcom_pull(task_ids='push_xcom', key='upd_geo_field_council_district') }}"),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

upd_geo_field_ward = BigQueryOperator(
    task_id='upd_geo_field_ward',
    sql=str("{{ ti.xcom_pull(task_ids='push_xcom', key='upd_geo_field_ward') }}"),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

upd_geo_field_fire = BigQueryOperator(
    task_id='upd_geo_field_fire',
    sql=str("{{ ti.xcom_pull(task_ids='push_xcom', key='upd_geo_field_fire_zone') }}"),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

upd_geo_field_police = BigQueryOperator(
    task_id='upd_geo_field_police',
    sql=str("{{ ti.xcom_pull(task_ids='push_xcom', key='upd_geo_field_police_zone') }}"),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

upd_geo_field_streets = BigQueryOperator(
    task_id='upd_geo_field_streets',
    sql=str("{{ ti.xcom_pull(task_ids='push_xcom', key='upd_geo_field_dpw_streets') }}"),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

upd_geo_field_env = BigQueryOperator(
    task_id='upd_geo_field_env',
    sql=str("{{ ti.xcom_pull(task_ids='push_xcom', key='upd_geo_field_dpw_enviro') }}"),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

upd_geo_field_parks = BigQueryOperator(
    task_id='upd_geo_field_parks',
    sql=str("{{ ti.xcom_pull(task_ids='push_xcom', key='upd_geo_field_dpw_parks') }}"),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

del_geo_view_ngh = BashOperator(
    task_id='del_geo_view_ngh',
    sql=str("{{ ti.xcom_pull(task_ids='push_xcom', key='del_geo_view_neighborhoods') }}"),
    dag=dag
)

del_geo_view_council = BashOperator(
    task_id='del_geo_view_council',
    sql=str("{{ ti.xcom_pull(task_ids='push_xcom', key='del_geo_view_council_districts') }}"),
    dag=dag
)

del_geo_view_ward = BashOperator(
    task_id='del_geo_view_ward',
    sql=str("{{ ti.xcom_pull(task_ids='push_xcom', key='del_geo_view_wards') }}"),
    dag=dag
)

del_geo_view_fire = BashOperator(
    task_id='del_geo_view_fire',
    sql=str("{{ ti.xcom_pull(task_ids='push_xcom', key='del_geo_view_fire_zones') }}"),
    dag=dag
)

del_geo_view_police = BashOperator(
    task_id='del_geo_view_police',
    sql=str("{{ ti.xcom_pull(task_ids='push_xcom', key='del_geo_view_police_zones') }}"),
    dag=dag
)

del_geo_view_streets = BashOperator(
    task_id='del_geo_view_streets',
    sql=str("{{ ti.xcom_pull(task_ids='push_xcom', key='del_geo_view_dpw_streets_divisions') }}"),
    dag=dag
)

del_geo_view_env = BashOperator(
    task_id='del_geo_view_env',
    sql=str("{{ ti.xcom_pull(task_ids='push_xcom', key='del_geo_view_dpw_es_divisions') }}"),
    dag=dag
)

del_geo_view_parks = BashOperator(
    task_id='del_geo_view_parks',
    sql=str("{{ ti.xcom_pull(task_ids='push_xcom', key='del_geo_view_dpw_parks_divisions') }}"),
    dag=dag
)

push_xcom >> init_temp_geo_table >> build_geo_view_ngh >> upd_geo_field_ngh >> del_geo_view_ngh
push_xcom >> init_temp_geo_table >> build_geo_view_council >> upd_geo_field_council >> del_geo_view_council
push_xcom >> init_temp_geo_table >> build_geo_view_ward >> upd_geo_field_ward >> del_geo_view_ward
push_xcom >> init_temp_geo_table >> build_geo_view_fire >> upd_geo_field_fire >> del_geo_view_fire
push_xcom >> init_temp_geo_table >> build_geo_view_police >> upd_geo_field_police >> del_geo_view_police
push_xcom >> init_temp_geo_table >> build_geo_view_streets >> upd_geo_field_streets >> del_geo_view_streets
push_xcom >> init_temp_geo_table >> build_geo_view_env >> upd_geo_field_env >> del_geo_view_env
push_xcom >> init_temp_geo_table >> build_geo_view_parks >> upd_geo_field_parks >> del_geo_view_parks
