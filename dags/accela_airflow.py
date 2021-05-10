from __future__ import absolute_import

import os

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_year, get_ds_month, default_args, build_revgeo_query, filter_old_values


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

accela_permits_dataflow = BashOperator(
    task_id='accela_permits_dataflow',
    bash_command="python {}/dependencies/dataflow_scripts/accela_dataflow.py --input gs://{}_"
                 "accela/permits/".format(os.environ['DAGS_PATH'], os.environ['GCS_PREFIX']) +
                 "{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ ds }}_permits.json --avro_output " +
                 "gs://{}_accela/permits/avro_output/".format(os.environ['GCS_PREFIX']) +
                 "{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ ds }}/",
    dag=dag
)

accela_permits_bq_load = GoogleCloudStorageToBigQueryOperator(
    task_id='accela_permits_bq_load',
    destination_project_dataset_table='{}:accela.permits_temp'.format(os.environ['GCLOUD_PROJECT']),
    bucket='{}_accela'.format(os.environ['GCS_PREFIX']),
    source_objects=["permits/avro_output/{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ ds }}/*.avro"],
    write_disposition='WRITE_APPEND',
    create_disposition='CREATE_IF_NEEDED',
    time_partitioning={'type': 'DAY'},
    source_format='AVRO',
    autodetect=True,
    dag=dag
)

accela_permits_dedup = BigQueryOperator(
    task_id='qalert_requests_bq_filter',
    sql=filter_old_values('accela', 'permits_temp', 'permits_raw', 'id'),
    use_legacy_sql=False,
    dag=dag
)

accela_permits_bq_merge = BigQueryOperator(
    task_id='accela_permits_bq_merge',
    sql='SELECT * FROM {}.accela.permits_temp'.format(os.environ['GCLOUD_PROJECT']),
    use_legacy_sql=False,
    destination_dataset_table='{}:accela.permits_raw'.format(os.environ['GCLOUD_PROJECT']),
    write_disposition='WRITE_APPEND',
    time_partitioning={'type': 'DAY'},
    dag=dag
)

accela_permits_geojoin = BigQueryOperator(
    task_id='accela_permits_geojoin',
    sql=build_revgeo_query('accela', 'permits_raw', 'id'),
    use_legacy_sql=False,
    destination_dataset_table='{}:accela.permits'.format(os.environ['GCLOUD_PROJECT']),
    write_disposition='WRITE_TRUNCATE',
    time_partitioning={'type': 'DAY'},
    dag=dag
)

accela_bq_drop_temp = BigQueryOperator(
    task_id='accela_bq_drop_temp',
    sql='DROP TABLE `{}.accela.permits_temp`'.format(os.environ['GCLOUD_PROJECT']),
    use_legacy_sql=False,
    dag=dag
)

accela_permits_beam_cleanup = BashOperator(
    task_id='accela_permits_beam_cleanup',
    bash_command=airflow_utils.beam_cleanup_statement('{}_accela'.format(os.environ['GCS_PREFIX'])),
    dag=dag
)

# The wprdc_export step places the full permits table into the export bucket. The data in the bucket are overwritten
# every DAG run. This is to ensure that if their extraction pipelines temporarily stop running, the entire dataset
# will be present and they will not need to backfill.

accela_wprdc_export = BigQueryToCloudStorageOperator(
  task_id='accela_wprdc_export',
  source_project_dataset_table='{}:accela.permits'.format(os.environ['GCLOUD_PROJECT']),
  destination_cloud_storage_uris='gs://{}_wprdc/accela_permits.csv'.format(os.environ['GCS_PREFIX']),
  dag=dag
)

accela_permits_gcs >> accela_permits_dataflow >> accela_permits_bq_load >> accela_permits_dedup >> \
    accela_permits_bq_merge >> accela_permits_geojoin >> accela_bq_drop_temp >> accela_permits_beam_cleanup >> accela_wprdc_export