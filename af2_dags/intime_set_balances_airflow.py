from __future__ import absolute_import

import os
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_month, get_ds_year, get_ds_day, default_args

dag = DAG(
    'intime_set_balances',
    default_args=default_args,
    schedule_interval=None,
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year,
                          'get_ds_day': get_ds_day},
    max_active_runs=1
)

set_balances_gcs = BashOperator(
    task_id='set_balances_gcs',
    bash_command=f"python {os.environ['GCS_LOADER_PATH']}/intime_set_balances_gcs.py",
    dag=dag
)

set_balances_gcs
