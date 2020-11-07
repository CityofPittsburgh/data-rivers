from __future__ import absolute_import

import os

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_year, get_ds_month, default_args, build_revgeo_query

# TODO: When Airflow 2.0 is released, upgrade the package, sub in DataFlowPythonOperator for BashOperator,
# and pass the argument 'py_interpreter=python3'

dag = DAG(
    'accela_permits',
    default_args=default_args,
    schedule_interval='@daily',
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year}
)

accela_permits_gcs = BashOperator(
    task_id='accela_permits_gcs',
    bash_command="python {}".format(os.environ['DAGS_PATH']) + "/dependencies/gcs_loaders"
                 "/accela_gcs.py --execution_date {{ ds }} --prev_execution_date {{ prev_ds }}",
    dag=dag
)

# accela_permits_dataflow = BashOperator(
#     task_id='accela_permits_dataflow',
#     bash_command="python {}/dependencies/dataflow_scripts/accela_dataflow.py --input gs://{}_"
#                  "accela/permits/".format(os.environ['DAGS_PATH'], os.environ['GCS_PREFIX']) +
#                  "{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ ds }}_permits.json --avro_output " +
#                  "gs://{}_accela/permits/avro_output/".format(os.environ['GCS_PREFIX']) +
#                  "{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ ds }}/",
#     dag=dag
# )

YEAR = '2016'

accela_permits_dataflow = BashOperator(
    task_id='accela_permits_dataflow',
    bash_command="python {}/dependencies/dataflow_scripts/accela_dataflow.py --input gs://{}_"
                 "accela/backfill/publicworks/".format(os.environ['DAGS_PATH'], os.environ['GCS_PREFIX']) +
                 "public_works_{}.json --avro_output ".format(YEAR) +
                 "gs://{}_accela/backfill/publicworks/avro_output/{}/".format(os.environ['GCS_PREFIX'], YEAR),
    dag=dag
)

# accela_permits_bq = GoogleCloudStorageToBigQueryOperator(
#     task_id='accela_permits_bq',
#     destination_project_dataset_table='{}:accela.permits_raw'.format(os.environ['GCLOUD_PROJECT']),
#     bucket='{}_accela'.format(os.environ['GCS_PREFIX']),
#     source_objects=["permits/avro_output/{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ ds }}/*.avro"],
#     write_disposition='WRITE_APPEND',
#     create_disposition='CREATE_IF_NEEDED',
#     time_partitioning={'type': 'DAY'},
#     source_format='AVRO',
#     autodetect=True,
#     dag=dag
# )

accela_permits_bq = GoogleCloudStorageToBigQueryOperator(
    task_id='accela_permits_bq',
    destination_project_dataset_table='{}:accela.permits_raw'.format(os.environ['GCLOUD_PROJECT']),
    bucket='{}_accela'.format(os.environ['GCS_PREFIX']),
    source_objects=["backfill/publicworks/avro_output/{}/*.avro".format(YEAR)],
    write_disposition='WRITE_APPEND',
    create_disposition='CREATE_IF_NEEDED',
    time_partitioning={'type': 'DAY'},
    source_format='AVRO',
    autodetect=True,
    dag=dag
)

accela_permits_geojoin = BigQueryOperator(
    task_id='accela_permits_geojoin',
    sql=build_revgeo_query('accela', 'permits_raw', 'id'),
    use_legacy_sql=False,
    destination_dataset_table='{}:accela.permits'.format(os.environ['GCLOUD_PROJECT']),
    write_disposition='WRITE_APPEND',
    time_partitioning={'type': 'DAY'},
    dag=dag
)

accela_permits_beam_cleanup = BashOperator(
    task_id='accela_permits_beam_cleanup',
    bash_command=airflow_utils.beam_cleanup_statement('{}_accela'.format(os.environ['GCS_PREFIX'])),
    dag=dag
)

# TODO: add steps to create temp table and remove ids from existing table that exist in that temp table from
# the permanent table before then appending the temp table to the permanent table (same pattern as qalert dag)

# accela_permits_gcs >> accela_permits_dataflow >> accela_permits_bq >> >> accela_permits_geojoin >> \
#     accela_permits_beam_cleanup

accela_permits_dataflow >> accela_permits_bq >> accela_permits_geojoin >> accela_permits_beam_cleanup
