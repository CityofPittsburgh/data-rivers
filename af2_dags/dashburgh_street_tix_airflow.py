        from __future__ import absolute_import

import os

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_month, get_ds_year, default_args

# The goal of this mini-DAG is to perform a weekly pull of all DPW Streets Maintenance tasks
# and store it in a table, which is then accessed by a Power BI chart to be displayed in the
# Dashburgh open data project

dag = DAG(
    'dashburgh_street_tix',
    default_args=default_args,
    schedule_interval='@daily',
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year}
)

format_street_tix = BashOperator(
    task_id='dashburgh_format_street_tix',
    bash_command=f"python {os.environ['SQL_SCRIPT_PATH']}/format_street_tix.py",
    dag=dag
)

qalert_beam_cleanup = BashOperator(
    task_id='dashburgh_street_tix_beam_cleanup',
    bash_command= airflow_utils.beam_cleanup_statement('{}_dashburgh_street_tix'.format(os.environ['GCS_PREFIX'])),
    dag=dag
)

format_street_tix >> qalert_beam_cleanup