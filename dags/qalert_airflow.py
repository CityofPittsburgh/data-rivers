from __future__ import absolute_import

import os

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
# from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator

from dependencies import airflow_utils
from dependencies.airflow_utils import build_revgeo_query, get_ds_year, get_ds_month, default_args, \
     format_gcs_call, format_dataflow_call, build_city_limits_query

# TODO: When Airflow 2.0 is released, upgrade the package, sub in DataFlowPythonOperator for BashOperator,
#  and pass the argument 'py_interpreter=python3'


dag = DAG(
        'qalert_requests',
        default_args = default_args,
        schedule_interval = '@daily',
        user_defined_filters = {'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year}
)


# run gcs_loader
qalert_requests_gcs = BashOperator(
        task_id = 'qalert_gcs',
        bash_command = format_gcs_call("qalert_gcs.py", ("{}".format(os.environ["GCS_PREFIX"]) + "_qalert"),
                                       "requests"),
        dag = dag
)

# run dataflow_script
qalert_requests_dataflow = BashOperator(
        task_id = 'qalert_dataflow',
        bash_command = format_dataflow_call("qalert_requests_dataflow.py"),
        dag = dag
)


# 1) load AVRO data (full dataset) from dataflow_script into BQ temp table
qalert_requests_bq = GoogleCloudStorageToBigQueryOperator(
        task_id = 'qalert_bq',
        destination_project_dataset_table = '{}:qalert.temp_new_req'.format(os.environ['GCLOUD_PROJECT']),
        bucket = '{}_qalert'.format(os.environ['GCS_PREFIX']),
        source_objects = ["requests/avro_output/{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ ds }}/*.avro"],
        write_disposition = 'WRITE_TRUNCATE',
        create_disposition = 'CREATE_IF_NEEDED',
        source_format = 'AVRO',
        autodetect = True,
        time_partitioning = {'type': 'DAY'},
        dag = dag
)

# TODO: update with lat/long cast as floats

# TODO: de dupe prior to first push to new_parent and new_child tix?

# 2) query new tickets to determine if they are in the city limits
qalert_requests_city_limits = BigQueryOperator(
        task_id = 'qalert_city_limits',
        sql = build_city_limits_query('qalert', 'temp_new_req', 'id'),
        use_legacy_sql = False,
        destination_dataset_table = '{}:qalert.temp_new_req'.format(os.environ['GCLOUD_PROJECT']),
        write_disposition = 'WRITE_APPEND',
        # create_disposition = 'CREATE_IF_NEEDED',
        time_partitioning = {'type': 'DAY'},
        dag = dag
)


# TODO: make sure the DPW districting matches the correct designation (change date to be verified with VAL)

# 3) join all the geo information (e.g. DPW districts, etc) to the new data
qalert_requests_geojoin = BigQueryOperator(
        task_id = 'qalert_geojoin',
        sql = build_revgeo_query('qalert', 'temp_new_req', 'id'),
        use_legacy_sql = False,
        destination_dataset_table = '{}:qalert.temp_new_req'.format(os.environ['GCLOUD_PROJECT']),
        write_disposition = 'WRITE_APPEND',
        # create_disposition = 'CREATE_IF_NEEDED',
        time_partitioning = {'type': 'DAY'},
        dag = dag
)

# 4) Append the geojoined to all_requests
append_query = ("INSERT INTO `{}:qalert.all_requests`".format(os.environ['GCLOUD_PROJECT']) +
                "SELECT * FROM `{}:qalert.temp_new_req`".format(os.environ['GCLOUD_PROJECT']))

qalert_requests_merge_new_tickets = BigQueryOperator(
        task_id = 'qalert_merge_new_tickets',
        sql = append_query,
        use_legacy_sql = False,
        destination_dataset_table = '{}:qalert.all_requests'.format(os.environ['GCLOUD_PROJECT']),
        write_disposition = 'WRITE_APPEND',
        # create_disposition = 'CREATE_IF_NEEDED',
        time_partitioning = {'type': 'DAY'},
        dag = dag
)

# 5a) BQ operator to split the data into parent view
make_new_parents_query = ("CREATE OR REPLACE VIEW {}.qalert.new_parents AS ".format(os.environ['GCLOUD_PROJECT']) +
                          "SELECT * FROM {}.qalert.temp_new_req ".format(os.environ['GCLOUD_PROJECT']) +
                          "WHERE child_ticket IS false")

qalert_requests_split_parent = BigQueryOperator(
        task_id = 'qalert_split_parent',
        sql = make_new_parents_query,
        use_legacy_sql = False,
        # destination_dataset_table = '{}:qalert.new_parents'.format(os.environ['GCLOUD_PROJECT']),
        write_disposition = 'WRITE_TRUNCATE',
        create_disposition = 'CREATE_IF_NEEDED',
        time_partitioning = {'type': 'DAY'},
        dag = dag
)

# 5b) BQ operator to split the data into child view
make_new_children_query = ("CREATE OR REPLACE VIEW {}.qalert.new_children AS ".format(os.environ['GCLOUD_PROJECT']) +
                           "SELECT * FROM {}.qalert.temp_new_req ".format(os.environ['GCLOUD_PROJECT']) +
                           "WHERE child_ticket IS true")

qalert_requests_split_child = BigQueryOperator(
        task_id = 'qalert_split_child',
        sql = make_new_children_query,
        use_legacy_sql = False,
        # destination_dataset_table = '{}:qalert.new_children'.format(os.environ['GCLOUD_PROJECT']),
        write_disposition = 'WRITE_TRUNCATE',
        create_disposition = 'CREATE_IF_NEEDED',
        time_partitioning = {'type': 'DAY'},
        dag = dag
)

# 6) Bash operator call the sql operations contained in python script
qalert_merge_related_requests = BashOperator(
        task_id = 'qalert_merge_related',
        bash_command = "python {}/dependencies/queries/qalert_merge_parent_child_requests.py",
        dag = dag
)

# 7) drop the temporary table from initial GCSToBQ upload
qalert_bq_drop_temp = BigQueryOperator(
        task_id = 'qalert_bq_drop_temp',
        sql = 'DROP TABLE `{}.qalert.temp_new_req`'.format(os.environ['GCLOUD_PROJECT']),
        use_legacy_sql = False,
        dag = dag
)

#TODO: push a scrubbed csv(?) to wprdc_gcs

qalert_beam_cleanup = BashOperator(
        task_id = 'qalert_beam_cleanup',
        bash_command = airflow_utils.beam_cleanup_statement('{}_qalert'.format(os.environ['GCS_PREFIX'])),
        dag = dag
)

qalert_requests_gcs >> qalert_requests_dataflow >> qalert_requests_bq >> qalert_requests_city_limits >> \
     qalert_requests_geojoin >> qalert_requests_merge_new_tickets >> qalert_requests_split_parent >> \
     qalert_requests_split_child >> qalert_merge_related_requests >> qalert_bq_drop_temp >> qalert_beam_cleanup
