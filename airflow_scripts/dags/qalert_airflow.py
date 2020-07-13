from __future__ import absolute_import

import os

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

from dependencies import airflow_utils
from dependencies.airflow_utils import build_revgeo_query, filter_old_values, get_ds_year, get_ds_month, default_args

# TODO: When Airflow 2.0 is released, upgrade the package, sub in DataFlowPythonOperator for BashOperator,
# and pass the argument 'py_interpreter=python3'

dag = DAG(
    'qalert',
    default_args=default_args,
    schedule_interval='@daily',
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year}
)

qalert_gcs = BashOperator(
    task_id='qalert_gcs',
    bash_command='python {}'.format(os.environ['DAGS_PATH'] + '/dependencies/gcs_loaders/qalert_gcs.py --since {{ '
                                                              'prev_ds }} --execution_date {{ ds }}'),
    dag=dag
)

# qalert_requests_dataflow = DataFlowPythonOperator(
#     task_id='qalert_requests_dataflow',
#     job_name='qalert_requests_dataflow',
#     py_file=(os.environ['DAGS_PATH'] + '/dependencies/dataflow_scripts/qalert_requests_dataflow.py'),
#     options={
#         "input": ("gs://{}_qalert/requests/".format(os.environ['GCS_PREFIX']) +
#                   "{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ ds }}_requests.json"),
#         "avro_output": ("gs://{}_qalert/requests/avro_output/".format(os.environ['GCS_PREFIX']) +
#                         "{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ ds }}")
#     },
#     dag=dag
# )

qalert_requests_dataflow = BashOperator(
    task_id='qalert_requests_dataflow',
    bash_command="python {}/dependencies/dataflow_scripts/qalert_requests_dataflow.py --input gs://{}_qalert/requests/"
                 .format(os.environ['DAGS_PATH'], os.environ['GCS_PREFIX']) + "{{ ds|get_ds_year }}/"
                 "{{ ds|get_ds_month }}/{{ ds }}_requests.json --avro_output " + "gs://{}_qalert/requests/avro_output/"
                 .format(os.environ['GCS_PREFIX']) + "{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ ds }}/",
    dag=dag
)

# qalert_activities_dataflow = DataFlowPythonOperator(
#     task_id='qalert_activities_dataflow',
#     job_name='qalert_activities_dataflow',
#     py_file=(os.environ['DAGS_PATH'] + '/dependencies/dataflow_scripts/qalert_activities_dataflow.py'),
#     options={
#         "input": ("gs://{}_qalert/activities/".format(os.environ['GCS_PREFIX']) +
#                   "{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ ds }}_activities.json"),
#         "avro_output": ("gs://{}_qalert/activities/avro_output/".format(os.environ['GCS_PREFIX']) +
#                         "{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ ds }}")
#     },
#     dag=dag
# )

qalert_activities_dataflow = BashOperator(
    task_id='qalert_activities_dataflow',
    bash_command="python {}/dependencies/dataflow_scripts/qalert_activities_dataflow.py --input gs://{}_qalert/"
                 "activities/".format(os.environ['DAGS_PATH'], os.environ['GCS_PREFIX']) + "{{ ds|get_ds_year }}/"
                 "{{ ds|get_ds_month }}/{{ ds }}_activities.json --avro_output " + "gs://{}_qalert/activities/"
                 "avro_output/".format(os.environ['GCS_PREFIX']) + "{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ ds }}/",
    dag=dag
)

qalert_activities_bq = GoogleCloudStorageToBigQueryOperator(
    task_id='qalert_activities_bq',
    destination_project_dataset_table='{}:qalert.activities'.format(os.environ['GCLOUD_PROJECT']),
    bucket='{}_qalert'.format(os.environ['GCS_PREFIX']),
    source_objects=["activities/avro_output/{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ ds }}/*.avro"],

    write_disposition='WRITE_APPEND',
    create_disposition='CREATE_IF_NEEDED',
    source_format='AVRO',
    autodetect=True,
    time_partitioning={'type': 'DAY'},
    dag=dag
)

# ^maybe a bug in this operator; you should not have to specify autodetect with an avro source

qalert_requests_bq = GoogleCloudStorageToBigQueryOperator(
    task_id='qalert_requests_bq',
    destination_project_dataset_table='{}:qalert.requests_temp'.format(os.environ['GCLOUD_PROJECT']),
    bucket='{}_qalert'.format(os.environ['GCS_PREFIX']),
    source_objects=["requests/avro_output/{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ ds }}/*.avro"],
    write_disposition='WRITE_APPEND',
    create_disposition='CREATE_IF_NEEDED',
    source_format='AVRO',
    autodetect=True,
    time_partitioning={'type': 'DAY'},
    dag=dag
)

qalert_requests_geo_join = BigQueryOperator(
    task_id='qalert_requests_bq_geojoin',
    sql=build_revgeo_query('qalert', 'requests_temp'),
    use_legacy_sql=False,
    destination_dataset_table='{}:qalert.requests_geo_temp'.format(os.environ['GCLOUD_PROJECT']),
    write_disposition='WRITE_APPEND',
    time_partitioning={'type': 'DAY'},
    dag=dag
)

qalert_requests_bq_filter = BigQueryOperator(
    task_id='qalert_requests_bq_filter',
    sql=filter_old_values('qalert', 'requests_geo_temp', 'requests', 'id'),
    use_legacy_sql=False,
    dag=dag
)

qalert_requests_bq_merge = BigQueryOperator(
    task_id='qalert_requests_bq_merge',
    sql='SELECT * FROM {}.qalert.requests_geo_temp'.format(os.environ['GCLOUD_PROJECT']),
    use_legacy_sql=False,
    destination_dataset_table='{}:qalert.requests'.format(os.environ['GCLOUD_PROJECT']),
    write_disposition='WRITE_APPEND',
    time_partitioning={'type': 'DAY'},
    dag=dag
)

qalert_bq_drop_temp = BigQueryOperator(
    task_id='qalert_bq_drop_temp',
    sql='DROP TABLE `{}.qalert.requests_temp`'.format(os.environ['GCLOUD_PROJECT']),
    use_legacy_sql=False,
    dag=dag
)

qalert_bq_drop_geo_temp = BigQueryOperator(
    task_id='qalert_bq_drop_geo_temp',
    sql='DROP TABLE `{}.qalert.requests_geo_temp`'.format(os.environ['GCLOUD_PROJECT']),
    use_legacy_sql=False,
    dag=dag
)

qalert_beam_cleanup = BashOperator(
    task_id='qalert_beam_cleanup',
    bash_command=airflow_utils.beam_cleanup_statement('{}_qalert'.format(os.environ['GCS_PREFIX'])),
    dag=dag
)

qalert_gcs >> qalert_requests_dataflow >> (qalert_requests_bq, qalert_beam_cleanup) >> qalert_requests_geo_join >> \
    qalert_requests_bq_filter >> qalert_requests_bq_merge >> (qalert_bq_drop_temp, qalert_bq_drop_geo_temp)

qalert_gcs >> qalert_activities_dataflow >> (qalert_activities_bq, qalert_beam_cleanup)
