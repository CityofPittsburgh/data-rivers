from __future__ import absolute_import


from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from dependencies.airflow_utils import default_args

# The goal of this


dag = DAG(
    'clear_junk_files',
    default_args=default_args,
    schedule_interval='@monthly',
)

clear_gcs_junk = BashOperator(
    task_id='clear_gcs_junk',
    bash_command="gsutil rm -a gs://pghpa_test_scratch/**",
    dag=dag
)


clear_bq_junk = BashOperator(
    task_id='clear_gbq_junk',
    bash_command="bq rm -f -r `data-rivers-testing`:scratch &&" \
                 "bq --location = US mk -d" \
                 "data-rivers-testing:scratch"
)

clear_gcs_junk >> clear_bq_junk