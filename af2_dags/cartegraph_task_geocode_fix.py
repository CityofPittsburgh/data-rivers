from __future__ import absolute_import

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_month, get_ds_year, default_args, \
    build_piecemeal_revgeo_query, build_dedup_old_updates

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

build_temp_geo_table_neighborhoods = BigQueryOperator(
    task_id='build_temp_geo_table_neighborhoods',
    sql=build_piecemeal_revgeo_query(dataset, raw_table, new_table, create_date, id_col, lat_field,
                                     long_field, geo_config[0]['geo_table'], geo_config[0]['geo_field']),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

build_temp_geo_table_council = BigQueryOperator(
    task_id='build_temp_geo_table_council',
    sql=build_piecemeal_revgeo_query(dataset, new_table, new_table, create_date, id_col, lat_field,
                                     long_field, geo_config[1]['geo_table'], geo_config[1]['geo_field']),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

build_temp_geo_table_ward = BigQueryOperator(
    task_id='build_temp_geo_table_ward',
    sql=build_piecemeal_revgeo_query(dataset, new_table, new_table, create_date, id_col, lat_field,
                                     long_field, geo_config[2]['geo_table'], geo_config[2]['geo_field']),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

build_temp_geo_table_fire = BigQueryOperator(
    task_id='build_temp_geo_table_fire',
    sql=build_piecemeal_revgeo_query(dataset, new_table, new_table, create_date, id_col, lat_field,
                                     long_field, geo_config[3]['geo_table'], geo_config[3]['geo_field']),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

build_temp_geo_table_police = BigQueryOperator(
    task_id='build_temp_geo_table_police',
    sql=build_piecemeal_revgeo_query(dataset, new_table, new_table, create_date, id_col, lat_field,
                                     long_field, geo_config[4]['geo_table'], geo_config[4]['geo_field']),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

build_temp_geo_table_streets = BigQueryOperator(
    task_id='build_temp_geo_table_streets',
    sql=build_piecemeal_revgeo_query(dataset, new_table, new_table, create_date, id_col, lat_field,
                                     long_field, geo_config[5]['geo_table'], geo_config[5]['geo_field']),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

build_temp_geo_table_env = BigQueryOperator(
    task_id='build_temp_geo_table_env',
    sql=build_piecemeal_revgeo_query(dataset, new_table, new_table, create_date, id_col, lat_field,
                                     long_field, geo_config[6]['geo_table'], geo_config[6]['geo_field']),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

build_temp_geo_table_parks = BigQueryOperator(
    task_id='build_temp_geo_table_parks',
    sql=build_piecemeal_revgeo_query(dataset, new_table, new_table, create_date, id_col, lat_field,
                                     long_field, geo_config[7]['geo_table'], geo_config[7]['geo_field']),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

# replace_src_table = BigQueryOperator(
#     task_id='replace_src_table',
#     sql=f'CREATE OR REPLACE TABLE `{os.environ["GCLOUD_PROJECT"]}.{dataset}.{raw_table}` '
#         f'AS SELECT * FROM `{os.environ["GCLOUD_PROJECT"]}.{dataset}.{new_table}`',
#     bigquery_conn_id='google_cloud_default',
#     use_legacy_sql=False,
#     dag=dag
# )

beam_cleanup = BashOperator(
    task_id='beam_cleanup',
    bash_command=airflow_utils.beam_cleanup_statement(f"{os.environ['GCS_PREFIX']}_{dataset}"),
    dag=dag
)

build_temp_geo_table_neighborhoods >> build_temp_geo_table_council >> build_temp_geo_table_ward >> \
build_temp_geo_table_fire >> build_temp_geo_table_police >> build_temp_geo_table_streets >> \
build_temp_geo_table_env >> build_temp_geo_table_parks >> beam_cleanup
# build_temp_geo_table >> replace_src_table >> beam_cleanup

# build_temp_geo_table_neighborhoods >> build_temp_geo_table_council >> beam_cleanup
# build_temp_geo_table_neighborhoods >> build_temp_geo_table_ward >> beam_cleanup
# build_temp_geo_table_neighborhoods >> build_temp_geo_table_fire >> beam_cleanup
# build_temp_geo_table_neighborhoods >> build_temp_geo_table_police >> beam_cleanup
# build_temp_geo_table_neighborhoods >> build_temp_geo_table_streets >> beam_cleanup
# build_temp_geo_table_neighborhoods >> build_temp_geo_table_env >> beam_cleanup
# build_temp_geo_table_neighborhoods >> build_temp_geo_table_parks >> beam_cleanup\

