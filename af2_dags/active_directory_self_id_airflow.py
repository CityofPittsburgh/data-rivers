from __future__ import absolute_import

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_month, get_ds_year, get_ds_day, default_args
import dependencies.bq_queries.general_queries as q

dag = DAG(
    'active_directory_self_id',
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2023, 12, 26),
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year,
                          'get_ds_day': get_ds_day},
    max_active_runs=1,
    catchup=False
)

f_name_query = F"""
SELECT ad_email, ad_fname, ceridian_fname
FROM `{os.environ['GCLOUD_PROJECT']}.active_directory.ad_ceridian_comparison`
WHERE ad_fname != ceridian_fname
AND ad_fname NOT LIKE '% Jr%'
AND ad_fname NOT LIKE '% _%'
AND ad_fname != CONCAT(ceridian_fname, 'P')
"""
export_f_name = BigQueryOperator(
    task_id='export_f_name',
    sql=q.direct_gcs_export(f"gs://{os.environ['GCS_PREFIX']}_active_directory/self_id/first_name_comp",
                            'csv', '*',  f_name_query),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

l_name_query = F"""
SELECT ad_email, ad_lname, ceridian_lname
FROM `{os.environ['GCLOUD_PROJECT']}.active_directory.ad_ceridian_comparison`
WHERE ad_lname != ceridian_lname
AND ad_lname NOT LIKE '%Jr%'
AND ad_lname NOT LIKE '% (%'
AND ad_lname NOT LIKE '% I%'
AND ceridian_lname NOT LIKE '%.'
"""
export_l_name = BigQueryOperator(
    task_id='export_l_name',
    sql=q.direct_gcs_export(f"gs://{os.environ['GCS_PREFIX']}_active_directory/self_id/last_name_comp",
                            'csv', '*', l_name_query),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

self_id_gcs = BashOperator(
    task_id='self_id_gcs',
    bash_command=f"python {os.environ['GCS_LOADER_PATH']}/active_directory_self_id_gcs.py",
    dag=dag
)

export_f_name >> self_id_gcs
export_l_name >> self_id_gcs
