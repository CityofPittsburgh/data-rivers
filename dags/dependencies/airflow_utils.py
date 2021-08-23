from __future__ import absolute_import

import logging
import os
import urllib

import pendulum
from datetime import datetime, timedelta
from google.cloud import bigquery, storage

# TODO:  Fix this import path 
# https://www.astronomer.io/guides/airflow-importing-custom-hooks-operators
# from ms_teams_webhook_operator import MSTeamsWebhookOperator


local_tz = pendulum.timezone('America/New_York')
dt = datetime.now(tz=local_tz)
yesterday = datetime.combine(dt - timedelta(1), datetime.min.time())

bq_client = bigquery.Client()
storage_client = storage.Client()


def on_failure(context):
    dag_id = context['dag_run'].dag_id

    task_id = context['task_instance'].task_id
    context['task_instance'].xcom_push(key=dag_id, value=True)

    logs_url = "{}/admin/airflow/log?dag_id={}&task_id={}&execution_date={}".format(
        os.environ['AIRFLOW_WEB_SERVER'], dag_id, task_id, context['ts'])
    utc_time = logs_url.split('T')[-1]
    logs_url = logs_url.replace(utc_time, urllib.parse.quote(utc_time))

    if os.environ['GCLOUD_PROJECT'] == 'data-rivers':
        teams_notification = MSTeamsWebhookOperator(
            task_id="msteams_notify_failure",
            trigger_rule="all_done",
            message="`{}` has failed on task: `{}`".format(dag_id, task_id),
            button_text="View log",
            button_url=logs_url,
            subtitle="View log: {}".format(logs_url),
            theme_color="FF0000",
            http_conn_id='msteams_webhook_url')

        teams_notification.execute(context)
        return
    else:
        pass


default_args = {
    'depends_on_past': False,
    'start_date': yesterday,
    'email': os.environ['EMAIL'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'project_id': os.environ['GCLOUD_PROJECT'],
    'on_failure_callback': on_failure,
    'dataflow_default_options': {
        'project': os.environ['GCLOUD_PROJECT']
    }
}


def get_ds_year(ds):
    return ds.split('-')[0]


def get_ds_month(ds):
    return ds.split('-')[1]


def build_revgeo_query(dataset, raw_table, id_field):
    """
    Take a table with lat/long values and reverse-geocode it into a new a final table. Use UNION to include rows that
    can't be reverse-geocoded in the final table. SELECT DISTINCT in both cases to remove duplicates.

    :param dataset: BigQuery dataset (string)
    :param raw_table: non-reverse geocoded table (string)
    :param id_field: field in table to use for deduplication
    :return: string to be passed through as arg to BigQueryOperator
    """
    return f"""
    WITH {raw_table}_geo AS 
    (
       SELECT DISTINCT
          {raw_table}.*,
          neighborhoods.hood AS neighborhood,
          council_districts.council AS council_district,
          wards.ward,
          fire_zones.firezones AS fire_zone,
          police_zones.zone AS police_zone,
          dpw_divisions.objectid AS dpw_division 
       FROM
          `{os.environ['GCLOUD_PROJECT']}.{dataset}.{raw_table}` AS {raw_table} 
          JOIN
             `data-rivers.geography.neighborhoods` AS neighborhoods 
             ON ST_CONTAINS(neighborhoods.geometry, ST_GEOGPOINT({raw_table}.long, {raw_table}.lat)) 
          JOIN
             `data-rivers.geography.council_districts` AS council_districts 
             ON ST_CONTAINS(council_districts.geometry, ST_GEOGPOINT({raw_table}.long, {raw_table}.lat)) 
          JOIN
             `data-rivers.geography.wards` AS wards 
             ON ST_CONTAINS(wards.geometry, ST_GEOGPOINT({raw_table}.long, {raw_table}.lat)) 
          JOIN
             `data-rivers.geography.fire_zones` AS fire_zones 
             ON ST_CONTAINS(fire_zones.geometry, ST_GEOGPOINT({raw_table}.long, {raw_table}.lat)) 
          JOIN
             `data-rivers.geography.police_zones` AS police_zones 
             ON ST_CONTAINS(police_zones.geometry, ST_GEOGPOINT({raw_table}.long, {raw_table}.lat)) 
          JOIN
             `data-rivers.geography.dpw_divisions` AS dpw_divisions 
             ON ST_CONTAINS(dpw_divisions.geometry, ST_GEOGPOINT({raw_table}.long, {raw_table}.lat))
    )
    SELECT
       * 
    FROM
       {raw_table}_geo 
    UNION ALL
    SELECT DISTINCT
       {raw_table}.*,
       CAST(NULL AS string) AS neighborhood,
       NULL AS council_district,
       NULL AS ward,
       CAST(NULL AS string) fire_zone,
       NULL AS police_zone,
       NULL AS dpw_division 
    FROM
       `{os.environ['GCLOUD_PROJECT']}.{dataset}.{raw_table}` AS {raw_table} 
    WHERE
       {raw_table}.{id_field} NOT IN 
       (
          SELECT
             {id_field} 
          FROM
             {raw_table}_geo
       )
    """


def dedup_table(dataset, table):
    return f"""
    SELECT DISTINCT * FROM `{os.environ['GCLOUD_PROJECT']}.{dataset}.{table}`
    """


def filter_old_values(dataset, temp_table, final_table, join_field):
    return f"""
    DELETE FROM `{os.environ['GCLOUD_PROJECT']}.{dataset}.{final_table}` final
    WHERE final.{join_field} IN (SELECT {join_field} FROM `{os.environ['GCLOUD_PROJECT']}.{dataset}.{temp_table}`)
    """


def beam_cleanup_statement(bucket):
    return "if gsutil -q stat gs://{}/beam_output/*; then gsutil rm gs://{}/beam_output/**; else echo " \
           "no beam output; fi".format(bucket, bucket)


def find_backfill_date(bucket_name, subfolder):
    """
    Return the date of the last time a given DAG was run when provided with a bucket name and
    GCS directory to search in. Iterates through buckets to find most recent file update date.
    :param bucket_name: name of GCS bucket (string)
    :param subfolder: name of directory within bucket_name to be searched (string)
    :return: date to be used as a backfill date when executing a new DAG run
    """
    ds = str(datetime.now())
    ds_yr = get_ds_year(ds)
    ds_month = get_ds_month(ds)
    upload_dates = []
    valid = False
    # only search back to 2017
    while not valid and int(ds_yr) > 2017:
        prefix = subfolder + '/' + ds_yr + '/' + ds_month
        blobs = storage_client.list_blobs(bucket_name, prefix=prefix)
        list_blobs = list(blobs)
        # if blobs are found
        if len(list_blobs):
            for blob in list_blobs:
                # determine if file size is greater than 0 kb
                if blob.size > 0:
                    # convert upload times to local time from UTC, then append to list
                    blob_date = datetime.astimezone(blob.time_created, local_tz)
                    upload_dates.append(blob_date)
            # if blobs greater than 0kb were appended then exit while loop
            if len(upload_dates) > 0:
                valid = True
            # if blobs were present, but no blobs that are greater than 0 kb detected search
            # backwards in time until 2017
            else:
                valid = False
                if ds_month != '01':
                    # values must be converted to int and zero padded to render vals < 10 as two digits
                    # for string comparison
                    ds_month = str(int(ds_month) - 1).zfill(2)
                else:
                    ds_yr = str(int(ds_yr) - 1)
                    ds_month = '12'
        # if no blobs detected search back until 2017
        else:
            valid = False
            if ds_month != '01':
                ds_month = str(int(ds_month) - 1).zfill(2)
            else:
                ds_yr = str(int(ds_yr) - 1)
                ds_month = '12'
    # extract the last run date by finding the largest upload date value to determine most recent date
    if len(upload_dates):
        last_run = max(upload_dates)
        return str(last_run.date())
    # if no valid dates found after loop finishes, return yesterday's date
    else:
        return str(yesterday.date())


def format_gcs_call(script_name, bucket_name, direc):
    exec_script_cmd = 'python {}'.format(os.environ['DAGS_PATH']) + '/dependencies/gcs_loaders/{}'.format(script_name)
    since_arg = ' --since {}'.format(find_backfill_date(bucket_name, direc))
    exec_date_arg = ' --execution_date {{ ds }}'
    return exec_script_cmd + since_arg + exec_date_arg


def format_dataflow_call(script_name):
    exec_script_cmd = 'python {}'.format(os.environ['DAGS_PATH']) + '/dependencies/dataflow_scripts/{}'.format(
            script_name)
    date_direc = "{{ds | get_ds_year}}/{{ds | get_ds_month}}/{{ds}}"
    input_arg = " --input gs://{}_qalert/requests/{}_requests.json".format(os.environ["GCS_PREFIX"], date_direc)
    output_arg = " --avro_output gs://{}_qalert/requests/avro_output/{}/".format(os.environ["GCS_PREFIX"], date_direc)
    return exec_script_cmd + input_arg + output_arg


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
