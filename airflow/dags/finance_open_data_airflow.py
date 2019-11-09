import os
import logging

from airflow import DAG, configuration, models
from airflow.operators.docker_operator import DockerOperator
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

YESTERDAY = datetime.combine(datetime.today() - timedelta(1), datetime.min.time())

default_args = {
    'depends_on_past': False,
    'start_date': YESTERDAY,
    'email': os.environ['EMAIL'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'project_id': os.environ['GCP_PROJECT'],
    'dataflow_default_options': {
        'project': os.environ['GCP_PROJECT'],
        'staging_location': 'gs://pghpa_finance/staging',
        'temp_location': 'gs://pghpa_finance/temp',
        'runner': 'DataflowRunner',
        'job_name': 'finance_open_data'
    }
}

dag = DAG(
    'finance_open_data', default_args=default_args, schedule_interval=timedelta(days=1))

gcs_load = DockerOperator(
    task_id='run_finance_image',
    image='gcr.io/data-rivers/pgh-finance',
    api_version='auto',
    auto_remove=True,
    environment={
        'ISAT_UN': os.environ['ISAT_UN'],
        'ISAT_PW': os.environ['ISAT_PW'],
        'GCS_AUTH_FILE': '/root/finance-open-data/data-rivers-service-acct.json'
    },
    dag=dag
)

dataflow_task = DataFlowPythonOperator(
    task_id='finance_dataflow',
    job_name='finance-dataflow',
    py_file=os.environ['FINANCE_DATAFLOW_FILE'],
    dag=dag
)

bq_load = PythonOperator(
    # load_avro_to_bq util function
)

beam_cleanup = PythonOperator(
    # util function to delete beam staging/temp files
)


gcs_load >> dataflow_task >> (bq_load, beam_cleanup)

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()