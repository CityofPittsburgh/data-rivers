from __future__ import absolute_import

import os
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_month, get_ds_year, get_ds_day, default_args

dag = DAG(
    'time_balance_backfill',
    default_args=default_args,
    schedule_interval=None,
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year,
                          'get_ds_day': get_ds_day},
    max_active_runs=1
)

exec_date = "{{ ds }}"
c_path = "accruals/backfill/{{ ds|get_ds_year }}"
c_loc = f"{c_path}/{exec_date}_backfill.json"
i_path = "timebank/backfill/{{ ds|get_ds_year }}"
i_loc = f"{i_path}/{exec_date}_backfill.json"

time_balance_backfill_gcs = BashOperator(
    task_id='time_balance_backfill_gcs',
    bash_command=f"python {os.environ['GCS_LOADER_PATH']}/time_balance_backfill_gcs.py --ceridian_output {c_loc} "
                 f"--intime_output {i_loc}",
    dag=dag
)

time_balance_backfill_gcs
