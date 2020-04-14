from __future__ import absolute_import

import os

from airflow import DAG, configuration, models
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from dependencies import airflow_utils
from dependencies.airflow_utils import yesterday, dt, build_revgeo_query, filter_old_values

# TODO: When Airflow 2.0 is released, upgrade the package, upgrade the virtualenv to Python3,
# and add the arg py_interpreter='python3' to DataFlowPythonOperator

# We set the start_date of the DAG to the previous date, as defined in airflow_utils. This will
# make the DAG immediately available for scheduling.

default_args = {
    'depends_on_past': False,
    'start_date': yesterday,
    'email': os.environ['EMAIL'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'project_id': os.environ['GCP_PROJECT'],
    'dataflow_default_options': {
        'project': os.environ['GCP_PROJECT']
    }
}

dag = DAG(
    'qalert', default_args=default_args, schedule_interval=timedelta(days=1))

qalert_gcs = BashOperator(
    task_id='qalert_gcs',
    bash_command='python {}'.format(os.getcwd() + '/airflow_scripts/dags/dependencies/gcs_loaders'
                                                  '/311_gcs.py'),
    dag=dag
)

qalert_requests_dataflow = BashOperator(
    task_id='qalert_requests_dataflow',
    bash_command='python {}'.format(os.getcwd() + '/airflow_scripts/dags/dependencies/dataflow_scripts'
                                                  '/qalert_requests_dataflow.py'),
    dag=dag
)

qalert_activities_dataflow = BashOperator(
    task_id='qalert_activities_dataflow',
    bash_command='python {}'.format(os.getcwd() + '/airflow_scripts/dags/dependencies/dataflow_scripts'
                                                  '/qalert_activities_dataflow.py'),
    dag=dag
)

qalert_activities_bq = GoogleCloudStorageToBigQueryOperator(
    task_id='qalert_activities_bq',
    destination_project_dataset_table='{}:311.activities'.format(os.environ['GCP_PROJECT']),
    bucket='{}_311'.format(os.environ['GCS_PREFIX']),
    source_objects=["activities/avro_output/{}/{}/{}/*.avro".format(dt.strftime('%Y'),
                                                                    dt.strftime('%m').lower(),
                                                                    dt.strftime("%Y-%m-%d"))],
    write_disposition='WRITE_APPEND',
    create_disposition='CREATE_IF_NEEDED',
    source_format='AVRO',
    time_partitioning={'type': 'DAY'},
    dag=dag
)

qalert_requests_bq = GoogleCloudStorageToBigQueryOperator(
    task_id='qalert_requests_bq',
    destination_project_dataset_table='{}:311.requests_temp'.format(os.environ['GCP_PROJECT']),
    bucket='{}_311'.format(os.environ['GCS_PREFIX']),
    source_objects=["requests/avro_output/{}/{}/{}/*.avro".format(dt.strftime('%Y'),
                                                                  dt.strftime('%m').lower(),
                                                                  dt.strftime("%Y-%m-%d"))],
    write_disposition='WRITE_APPEND',
    create_disposition='CREATE_IF_NEEDED',
    source_format='AVRO',
    time_partitioning={'type': 'DAY'},
    dag=dag
)

qalert_requests_geo_join = BigQueryOperator(
    task_id='qalert_requests_bq_geojoin',
    sql=build_revgeo_query('311', 'requests_temp'),
    use_legacy_sql=False,
    destination_dataset_table='{}:311.requests_geo_temp'.format(os.environ['GCP_PROJECT']),
    write_disposition='WRITE_APPEND',
    time_partitioning={'type': 'DAY'},
    dag=dag
)

qalert_requests_bq_filter = BigQueryOperator(
    task_id='qalert_requests_bq_filter',
    sql=filter_old_values('311', 'requests_geo_temp', 'requests', 'id'),
    use_legacy_sql=False,
    dag=dag
)

qalert_requests_bq_merge = BigQueryOperator(
    task_id='qalert_requests_bq_merge',
    sql='SELECT * FROM {}.311.requests_geo_temp'.format(os.environ['GCP_PROJECT']),
    use_legacy_sql=False,
    destination_dataset_table='{}:311.requests'.format(os.environ['GCP_PROJECT']),
    write_disposition='WRITE_APPEND',
    time_partitioning={'type': 'DAY'},
    dag=dag
)

qalert_bq_drop_temp = BigQueryOperator(
    task_id='qalert_bq_drop_temp',
    sql='DROP TABLE `{}.311.requests_temp`'.format(os.environ['GCP_PROJECT']),
    use_legacy_sql=False,
    dag=dag
)

qalert_bq_drop_geo_temp = BigQueryOperator(
    task_id='qalert_bq_drop_geo_temp',
    sql='DROP TABLE `{}.311.requests_geo_temp`'.format(os.environ['GCP_PROJECT']),
    use_legacy_sql=False,
    dag=dag
)

beam_cleanup = BashOperator(
    task_id='qalert_beam_cleanup',
    bash_command=airflow_utils.beam_cleanup_statement('{}_311'.format(os.environ['GCS_PREFIX'])),
    dag=dag
)

qalert_gcs >> qalert_requests_dataflow >> (qalert_requests_bq, beam_cleanup) >> qalert_requests_geo_join >> \
    qalert_requests_bq_filter >> qalert_requests_bq_merge >> (qalert_bq_drop_temp, qalert_bq_drop_geo_temp)

qalert_gcs >> qalert_activities_dataflow >> (qalert_activities_bq, beam_cleanup)
