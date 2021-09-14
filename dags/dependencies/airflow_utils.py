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


def build_revgeo_query(dataset, raw_table, id_field, create_date='create_date_est', lat_field='lat', long_field='long'):
    """
    Take a table with lat/long values and reverse-geocode it into a new a final table. Use UNION to include rows that
    can't be reverse-geocoded in the final table. SELECT DISTINCT in both cases to remove duplicates.

    :param dataset: BigQuery dataset (string)
    :param raw_table: non-reverse geocoded table (string)
    :param id_field: field in table to use for deduplication
    :param create_date: ticket creation date (string)
    :param lat_field: field in table that identifies latitude value
    :param long_field: field in table that identifies longitude value
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
          dpw_streets_divisions.division AS dpw_streets,
          dpw_es_divisions.dpwesid AS dpw_enviro,
          dpw_parks_divisions.division AS dpw_parks
       FROM
          `{os.environ['GCLOUD_PROJECT']}.{dataset}.{raw_table}` AS {raw_table} 
          JOIN
             `data-rivers.geography.neighborhoods` AS neighborhoods 
             ON ST_CONTAINS(neighborhoods.geometry, ST_GEOGPOINT({raw_table}.{long_field}, {raw_table}.{lat_field})) 
          JOIN
             `data-rivers.geography.council_districts` AS council_districts 
             ON ST_CONTAINS(council_districts.geometry, ST_GEOGPOINT({raw_table}.{long_field}, {raw_table}.{lat_field})) 
          JOIN
             `data-rivers.geography.wards` AS wards 
             ON ST_CONTAINS(wards.geometry, ST_GEOGPOINT({raw_table}.{long_field}, {raw_table}.{lat_field})) 
          JOIN
             `data-rivers.geography.fire_zones` AS fire_zones 
             ON ST_CONTAINS(fire_zones.geometry, ST_GEOGPOINT({raw_table}.{long_field}, {raw_table}.{lat_field})) 
          JOIN
             `data-rivers.geography.police_zones` AS police_zones 
             ON ST_CONTAINS(police_zones.geometry, ST_GEOGPOINT({raw_table}.{long_field}, {raw_table}.{lat_field})) 
          JOIN
             `data-rivers.geography.dpw_streets_divisions` AS dpw_streets_divisions 
             ON ST_CONTAINS(dpw_streets_divisions.geometry, ST_GEOGPOINT({raw_table}.{long_field}, {raw_table}.{lat_field}))
             AND TIMESTAMP(PARSE_DATE('%m/%d/%Y', start_date)) <= TIMESTAMP(temp_new_req.{create_date})
             AND IFNULL(TIMESTAMP(PARSE_DATE('%m/%d/%Y', end_date)), CURRENT_TIMESTAMP()) >= TIMESTAMP(temp_new_req.{create_date})
          JOIN
             `data-rivers.geography.dpw_es_divisions` AS dpw_es_divisions 
             ON ST_CONTAINS(dpw_es_divisions.geometry, ST_GEOGPOINT({raw_table}.{long_field}, {raw_table}.{lat_field}))
             AND TIMESTAMP(PARSE_DATE('%m/%d/%Y', start_date)) <= TIMESTAMP(temp_new_req.{create_date})
             AND IFNULL(TIMESTAMP(PARSE_DATE('%m/%d/%Y', end_date)), CURRENT_TIMESTAMP()) >= TIMESTAMP(temp_new_req.{create_date})
          JOIN
             `data-rivers.geography.dpw_parks_divisions` AS dpw_parks_divisions 
             ON ST_CONTAINS(dpw_parks_divisions.geometry, ST_GEOGPOINT({raw_table}.{long_field}, {raw_table}.{lat_field}))
             AND TIMESTAMP(PARSE_DATE('%m/%d/%Y', start_date)) <= TIMESTAMP(temp_new_req.{create_date})
             AND IFNULL(TIMESTAMP(PARSE_DATE('%m/%d/%Y', end_date)), CURRENT_TIMESTAMP()) >= TIMESTAMP(temp_new_req.{create_date})
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
       NULL AS dpw_streets,
       NULL AS dpw_enviro,
       NULL AS dpw_parks 
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
    :param ds - date derived from airflow built-in
    :return: date to be used as a backfill date when executing a new DAG run
    """
    dt_yr = str(dt).split('-')[0]
    dt_month = str(dt).split('-')[1]
    upload_dates = []
    valid = False

    # only search back to 2017
    while not valid and int(dt_yr) > 2017:
        prefix = subfolder + '/' + dt_yr + '/' + dt_month
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
                if dt_month != '01':
                    # values must be converted to int and zero padded to render vals < 10 as two digits
                    # for string comparison
                    dt_month = str(int(dt_month) - 1).zfill(2)
                else:
                    dt_yr = str(int(dt_yr) - 1)
                    dt_month = '12'
        # if no blobs detected search back until 2017
        else:
            valid = False
            if dt_month != '01':
                dt_month = str(int(dt_month) - 1).zfill(2)
            else:
                dt_yr = str(int(dt_yr) - 1)
                dt_month = '12'
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
    exec_date_arg = ' --execution_date {}'.format(dt.date())
    return exec_script_cmd + since_arg + exec_date_arg


def format_dataflow_call(script_name):
    exec_script_cmd = 'python {}'.format(os.environ['DAGS_PATH']) + '/dependencies/dataflow_scripts/{}'.format(
            script_name)

    if dt.month < 10:
        month = '0'+ str(dt.month)
    date_direc = "{}/{}/{}".format(dt.year, month, dt.date())

    input_arg = " --input gs://{}_qalert/requests/{}_requests.json".format(os.environ["GCS_PREFIX"], date_direc)
    output_arg = " --avro_output gs://{}_qalert/requests/avro_output/{}/".format(os.environ["GCS_PREFIX"], date_direc)
    return exec_script_cmd + input_arg + output_arg


def build_city_limits_query(dataset, raw_table, lat_field='lat', long_field='long'):
    """
    Determine whether a set of coordinates fall within the borders of the City of Pittsburgh,
    while also falling outside the borders of Mt. Oliver. If an address is within the city,
    the address_type field is left as-is. Otherwise, address_type is changed to 'Outside
    of City'.
    :param dataset: source BigQuery dataset that contains the table to be updated
    :param raw_table: name of the table containing raw 311 data
    :param: lat_field: name of table column that contains latitude value
    :param: long_field: name of table column that contains longitude value
    :return: string to be passed through as arg to BigQueryOperator
    **NOTE**: A strange issue occurs with the Mt Oliver borders if it is stored in BigQuery in GEOGRAPHY format.
    All lat/longs outside of the Mt Oliver boundaries are identified as inside Mt Oliver when passed through ST_COVERS,
    and all lat/longs inside of Mt Oliver are identified as outside of it. To get around this problem, we have stored
    the boundary polygons as strings, and then convert those strings to polygons using ST_GEOGFROMTEXT.
    """
    return f"""
    UPDATE {dataset}.{raw_table}
    SET address_type = IF ( (
       ST_COVERS((ST_GEOGFROMTEXT(SELECT geometry FROM `data-rivers.geography.pittsburgh_and_mt_oliver_borders`
                                  WHERE city = 'Pittsburgh')),
                   ST_GEOGPOINT({raw_table}.{long_field}, {raw_table}.{lat_field}))
        AND NOT 
        ST_COVERS((ST_GEOGFROMTEXT(SELECT geometry FROM `data-rivers.geography.pittsburgh_and_mt_oliver_borders`
                                   WHERE city = 'Mt. Oliver')),
                   ST_GEOGPOINT({raw_table}.{long_field}, {raw_table}.{lat_field}))
       ), 'Outside of City', address_type )
    WHERE {raw_table}.{long_field} IS NOT NULL AND {raw_table}.{lat_field} IS NOT NULL
    """


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
