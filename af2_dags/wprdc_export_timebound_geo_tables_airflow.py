from __future__ import absolute_import

from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

from dependencies.airflow_utils import default_args


dag = DAG(
        'export_geo_tables_to_wprdc',
        default_args = default_args,
        schedule_interval = '30 1 1,15 * *',
        start_date = datetime(2022, 12, 1),
        catchup = False,
        max_active_runs = 1
)


# Run gcs_loader
export_tables_to_csv_from_shell = BashOperator(
        task_id = 'export_tables_to_csv_from_shell',
        bash_command = './dependencies/shell_scripts/export_timebound_geo_tables_wprdc.sh',
        dag = dag
)


export_tables_to_csv_from_shell


