from __future__ import absolute_import

from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from dependencies.airflow_utils import default_args

# The goal of this DAG is to clear out junk files which accumulate in data-rivers-testing buckets/datasets as a
# matter of routine development tests and procedures. This will run monthly and delete all files in the
# bucket/dataset. The BQ dataset is destroy and created again because there is currently (12/22) not a CLI method to
# delete all tables. The other option currently available would be to loop thru the tables and delete them one at a
# time.  


dag = DAG(
    'clear_junk_files',
    default_args=default_args,
    start_date = datetime(2022, 12, 1),
    schedule_interval='@monthly',
)

clear_gcs_junk = BashOperator(
    task_id='clear_gcs_junk',
    bash_command="gsutil rm -a gs://pghpa_test_scratch/**",
    dag=dag
)


clear_bq_junk = BashOperator(
    task_id='clear_bq_junk',
    bash_command="bq rm -f -r `data-rivers-testing`:scratch &&" \
                 " bq --location='US' mk -d" \
                 " data-rivers-testing:scratch"
)

clear_gcs_junk >> clear_bq_junk