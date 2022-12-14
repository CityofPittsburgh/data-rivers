from __future__ import absolute_import

from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from dependencies.airflow_utils import default_args

# The goal of this DAG is to clear out junk files which accumulate in
# data-rivers-testing/data-rivers/data-confluence-314416/data-bridgis buckets/datasets as a matter of routine
# development tests and procedures. This will run monthly and delete all files in the bucket/dataset. The BQ dataset
# is destroy and created again because there is currently (12/22) not a CLI method to delete all tables. The other
# option currently available would be to loop thru the tables and delete them one at a time.  Similarly, the DAG does
# not work well if the scratch GCS bucket is empty. Thus, this is destroyed and reated again.


dag = DAG(
    'clear_scratch_datasets',
    default_args=default_args,
    start_date = datetime(2022, 1, 1),
    schedule_interval='@monthly'
)


def init_cmds_xcomm(**kwargs):
    """Pushes an XCom used by several targgets. Used to create a dict with all runtime cmds. This cleans up the
    readability of the bash commands etc"""

    proj_info = [["data-rivers", "pghpa_scratch", "scratch"], ["data-rivers-testing", "pghpa_test_scratch", "scratch"],
                 ["data-confluence-314416","confluence_scratch", "scratch"], ["data-bridgis","pghpa_gis_scratch",
                                                                          "scratch"]]

    for p in proj_info:
        # build all unique GCS clean up bash commands
        gcs_cmd_str = F"gcloud config set project {p[0]} && gsutil rm -r gs://{p[1]}/ && gsutil mkdir gs://{p[1]}"
        kwargs['ti'].xcom_push(key = F"gcs_cmd_str_{p[0]}", value = gcs_cmd_str)


        # build all unique BQ clean up bash commands
        bq_cmd_str = F"gcloud config set project {p[0]} && bq rm -f -r `{p[0]}`:scratch &&  bq --location='US' mk -d" \
                     F" `{p[0]}`:scratch"
        kwargs['ti'].xcom_push(key = F"bq_cmd_str_{p[0]}", value = bq_cmd_str)


push_xcom = PythonOperator(
        task_id = 'push_xcom',
        python_callable = init_cmds_xcomm,
        dag = dag
)
# provide_context = True,

clear_dr_gcs_junk = BashOperator(
    task_id='clear_dr_gcs_junk',
    bash_command = str("{{ ti.xcom_pull(task_ids = 'push_xcom', key='gcs_cmd_str_data-rivers') }}"),
    dag=dag
)


clear_drt_gcs_junk = BashOperator(
    task_id='clear_drt_gcs_junk',
    bash_command = str("{{ ti.xcom_pull(task_ids = 'push_xcom', key='gcs_cmd_str_data-rivers-testing') }}"),
    dag=dag
)


clear_dc_gcs_junk = BashOperator(
    task_id='clear_dc_gcs_junk',
    bash_command = str("{{ ti.xcom_pull(task_ids = 'push_xcom', key='gcs_cmd_str_data-confluence-314416') }}"),
    dag=dag
)


clear_gis_gcs_junk = BashOperator(
    task_id='clear_gis_gcs_junk',
    bash_command = str("{{ ti.xcom_pull(task_ids = 'push_xcom', key='gcs_cmd_str_data-bridgis') }}"),
    dag=dag
)


clear_dr_bq_junk = BashOperator(
    task_id='clear_dr_bq_junk',
    bash_command = str("{{ ti.xcom_pull(task_ids = 'push_xcom', key='bq_cmd_str_data-rivers') }}"),
    dag = dag
)


clear_drt_bq_junk = BashOperator(
    task_id='clear_drt_bq_junk',
    bash_command = str("{{ ti.xcom_pull(task_ids = 'push_xcom', key='bq_cmd_str_data-rivers-testing') }}"),
    dag = dag
)


clear_dc_bq_junk = BashOperator(
    task_id='clear_dc_bq_junk',
    bash_command = str("{{ ti.xcom_pull(task_ids = 'push_xcom', key='bq_cmd_str_data-confluence-314416') }}"),
    dag=dag
)


clear_gis_bq_junk = BashOperator(
    task_id='clear_gis_bq_junk',
    bash_command = str("{{ ti.xcom_pull(task_ids = 'push_xcom', key='bq_cmd_str_data-bridgis') }}"),
    dag=dag
)


push_xcom >> clear_dr_gcs_junk
push_xcom >> clear_drt_gcs_junk
push_xcom >> clear_dc_gcs_junk
push_xcom >> clear_gis_gcs_junk

push_xcom >> clear_dr_bq_junk
push_xcom >> clear_drt_bq_junk
push_xcom >> clear_dc_bq_junk
push_xcom >> clear_gis_bq_junk