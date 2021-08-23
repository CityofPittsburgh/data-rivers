from __future__ import absolute_import

import os

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_year, get_ds_month, default_args, format_dataflow_call, format_gcs_call

# TODO: When Airflow 2.0 is released, upgrade the package, sub in DataFlowPythonOperator for BashOperator,
# and pass the argument 'py_interpreter=python3'

dag = DAG(
    'test',
    default_args=default_args,
    schedule_interval='@daily',
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year}
)

qalert_gcs = BashOperator(
    task_id='qalert_gcs',
    bash_command=format_gcs_call("qalert_gcs.p", "pghpa_test_qalert", "requests"),
    dag=dag
)

qalert_requests_dataflow = BashOperator(
    task_id='qalert_requests_dataflow',
    bash_command=format_dataflow_call("qalert_requests_dataflow.py"),
    dag=dag
)

qalert_beam_cleanup = BashOperator(
    task_id='qalert_beam_cleanup',
    bash_command=airflow_utils.beam_cleanup_statement('{}_qalert'.format(os.environ['GCS_PREFIX'])),
    dag=dag
)

qalert_gcs >> qalert_requests_dataflow >> qalert_beam_cleanup