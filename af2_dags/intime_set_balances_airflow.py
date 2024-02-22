from __future__ import absolute_import

import os
import io
import pandas as pd
from datetime import datetime, timedelta, date
from google.cloud import storage

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_month, get_ds_year, get_ds_day, default_args, gcs_to_email, log_task
import dependencies.bq_queries.general_queries as g_q
import dependencies.bq_queries.employee_admin.ceridian_admin as c_q

dag = DAG(
    'intime_set_balances',
    default_args=default_args,
    schedule_interval='0 17 * * *',
    start_date=datetime(2023, 12, 8),
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year,
                          'get_ds_day': get_ds_day},
    max_active_runs=1,
    catchup=False
)

exec_date = "{{ ds }}"
path = "timebank/update_log/{{ ds|get_ds_year }}/{{ ds|get_ds_month }}"
json_loc = f"{path}/{exec_date}_updates.json"


def branch_time_balance_comp(offset):
    storage_client = storage.Client()
    today = datetime.today()
    if date.today().weekday() == 2:
        return ['export_discrepancy']
    elif date.today().weekday() == 4:
        offset_val = timedelta(days=offset)
        comp_date = today + offset_val
        comp_str = comp_date.strftime("%m/%d/%Y")

        bucket = storage_client.get_bucket(f"{os.environ['GCS_PREFIX']}_ceridian")
        blob = bucket.blob("timekeeping/payroll_schedule_23-24.csv")
        content = blob.download_as_string()
        stream = io.StringIO(content.decode(encoding='utf-8'))
        sched_df = pd.read_csv(stream)
        if comp_str in list(sched_df['pay_issued']):
            return ['export_for_api']
        else:
            return ['irrelevant_day']
    elif date.today().weekday() == 5:
        return ['export_for_api']
    else:
        return ['irrelevant_day']


choose_branch = BranchPythonOperator(
    task_id='choose_branch',
    python_callable=branch_time_balance_comp,
    op_args=[7],
    dag=dag
)

export_discrepancy = BigQueryOperator(
    task_id='export_discrepancy',
    sql=g_q.direct_gcs_export(f"gs://{os.environ['GCS_PREFIX']}_ceridian/data_sharing/discrepancy_report",
                              'csv', '*',  c_q.compare_timebank_balances('discrepancy_report', -2)),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

email_comparison = PythonOperator(
    task_id='email_comparison',
    python_callable=gcs_to_email,
    op_kwargs={"bucket": f"{os.environ['GCS_PREFIX']}_ceridian",
               "file_path": "data_sharing/discrepancy_report000000000000.csv",
               "recipients": [os.environ["INTIME_ALERT_EMAIL"]],
               "cc": [os.environ["EMAIL"], os.environ["HR_ALERT_EMAIL"]],
               "subject": "ALERT: Time Bank Discrepancy Report",
               "message": "Attached is an extract of all time bank balances that differ between the Ceridian and "
                          "InTime source systems.",
               "attachment_name": "discrepancy_report"},
    dag=dag
)

export_fields = """
employee_id AS `Employee ID`, code AS `Time Bank Reference`,
CAST(retrieval_date AS STRING FORMAT 'MM/DD/YYYY') AS `Set Balance Date`,
ceridian_balance AS Balance, NULL AS `Time Bank Effective Date`,
NULL AS `Accrual Ref`, NULL AS `Worked Hours Ref`, NULL AS `Balance Reset Ref`
"""
export_for_api = BigQueryOperator(
    task_id='export_for_api',
    sql=g_q.direct_gcs_export(f'gs://{os.environ["GCS_PREFIX"]}_intime/timebank/time_balance_mismatches',
                              'csv', export_fields, c_q.compare_timebank_balances('balance_update')),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

set_balances_gcs = BashOperator(
    task_id='set_balances_gcs',
    bash_command=f"python {os.environ['GCS_LOADER_PATH']}/intime_set_balances_gcs.py --output_arg {json_loc}",
    dag=dag
)

irrelevant_day = PythonOperator(
    task_id='irrelevant_day',
    python_callable=log_task,
    op_kwargs={"dag_id": "intime_set_balances",
               "message": f"No comparison performed between Ceridian & InTime time balances on date {exec_date}. "
                          f"Comparisons are only performed on Wednesdays and non-payday Fridays."},
    dag=dag
)

choose_branch >> export_discrepancy >> email_comparison
choose_branch >> export_for_api >> set_balances_gcs
choose_branch >> irrelevant_day
