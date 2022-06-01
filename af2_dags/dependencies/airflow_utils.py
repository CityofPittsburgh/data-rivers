from __future__ import absolute_import

import logging
import os
import urllib
import ndjson

import pendulum
from datetime import datetime, timedelta
from google.cloud import bigquery, storage

# TODO:  Fix this import path 
# https://www.astronomer.io/guides/airflow-importing-custom-hooks-operators
# from ms_teams_webhook_operator import MSTeamsWebhookOperator


local_tz = pendulum.timezone('America/New_York')
dt = datetime.now(tz = local_tz)
yesterday = datetime.combine(dt - timedelta(1), datetime.min.time())

bq_client = bigquery.Client()
storage_client = storage.Client()


def on_failure(context):
    dag_id = context['dag_run'].dag_id

    task_id = context['task_instance'].task_id
    context['task_instance'].xcom_push(key = dag_id, value = True)

    logs_url = "{}/admin/airflow/log?dag_id={}&task_id={}&execution_date={}".format(
            os.environ['AIRFLOW_WEB_SERVER'], dag_id, task_id, context['ts'])
    utc_time = logs_url.split('T')[-1]
    logs_url = logs_url.replace(utc_time, urllib.parse.quote(utc_time))

    if os.environ['GCLOUD_PROJECT'] == 'data-rivers':
        teams_notification = MSTeamsWebhookOperator(
                task_id = "msteams_notify_failure",
                trigger_rule = "all_done",
                message = "`{}` has failed on task: `{}`".format(dag_id, task_id),
                button_text = "View log",
                button_url = logs_url,
                subtitle = "View log: {}".format(logs_url),
                theme_color = "FF0000",
                http_conn_id = 'msteams_webhook_url')

        teams_notification.execute(context)
        return
    else:
        pass


default_args = {
        'depends_on_past'         : False,
        'start_date'              : yesterday,
        'email'                   : os.environ['EMAIL'],
        'email_on_failure'        : True,
        'email_on_retry'          : False,
        'retries'                 : 1,
        'retry_delay'             : timedelta(minutes = 5),
        'project_id'              : os.environ['GCLOUD_PROJECT'],
        'on_failure_callback'     : on_failure,
        'dataflow_default_options': {
                'project': os.environ['GCLOUD_PROJECT']
        }
}


def get_ds_year(ds):
    return ds.split('-')[0]


def get_ds_month(ds):
    return ds.split('-')[1]


def get_ds_day(ds):
    return ds.split('-')[2]


# TODO: phase out the usage of build_revgeo_query() in favor of build_rev_geo_time_bound_query()
def build_revgeo_time_bound_query(dataset, raw_table, new_table_name, create_date, id_col, lat_field, long_field):
    """
    Take a table with lat/long values and reverse-geocode it into a new a final table.
    This function is a substantial refactor of the build_rev_geo() function. This query allows a lat/long point to be
    localized to a zone within a specifc time range. This is critical as some boundaries have changed over time.

    Since boundaries change over time, this query builds upon a previous version, by selecting the boundaries at a
    relevant time point.


    :param dataset: BigQuery dataset (string)
    :param raw_table: starting table that has not been geocoded yet (string)
    :param id_col: field in table to use for deduplication
    :param create_date: ticket creation date (string)
    :param lat_field: field in table that identifies latitude value
    :param long_field: field in table that identifies longitude value
    :return: string to be passed through as arg to BigQueryOperator
    """

    return f"""
    CREATE OR REPLACE TABLE `{os.environ["GCLOUD_PROJECT"]}.{dataset}.{new_table_name}` AS

    -- return zones for all records that it is possible to rev geocode. some records will not be possible to process 
    (bad lat/long etc) and will be pulled in via the next blocked
    WITH
      sel_zones AS (
      SELECT
        raw.{id_col},
        CAST (t_hoods.zone AS STRING) AS neighborhood_name,
        CAST (t_cd.zone AS STRING) AS council_district,
        CAST (t_w.zone AS STRING) AS ward,
        CAST (t_fz.zone AS STRING) AS fire_zone,
        CAST (t_pz.zone AS STRING) AS police_zone,
        CAST (t_st.zone AS STRING) AS dpw_streets,
        CAST (t_es.zone AS STRING) AS dpw_enviro,
        CAST (t_pk.zone AS STRING) AS dpw_parks
      FROM
        `{os.environ["GCLOUD_PROJECT"]}.{dataset}.{raw_table}` raw

      -- neighborhoods
      JOIN `data-rivers.timebound_geography.neighborhoods` AS t_hoods ON
        ST_CONTAINS(t_hoods.geometry, ST_GEOGPOINT(raw.{long_field}, raw.{lat_field}))
        AND TIMESTAMP(PARSE_DATETIME('%m/%d/%Y %H:%M:%S-00:00',t_hoods.start_date)) <= TIMESTAMP(raw.{create_date})
        AND IFNULL(TIMESTAMP(PARSE_DATETIME('%m/%d/%Y %H:%M:%S-00:00', t_hoods.end_date)),
            CURRENT_TIMESTAMP()) >= TIMESTAMP(raw.{create_date})


      -- council districts
      JOIN `data-rivers.timebound_geography.council_districts` AS t_cd ON
        ST_CONTAINS(t_cd.geometry, ST_GEOGPOINT(raw.{long_field}, raw.{lat_field}))
        AND TIMESTAMP(PARSE_DATETIME('%m/%d/%Y %H:%M:%S-00:00',t_cd.start_date)) <= TIMESTAMP(raw.{create_date})
        AND IFNULL(TIMESTAMP(PARSE_DATETIME('%m/%d/%Y %H:%M:%S-00:00', t_cd.end_date)),
            CURRENT_TIMESTAMP()) >= TIMESTAMP(raw.{create_date})

      -- wards
      JOIN `data-rivers.timebound_geography.wards` AS t_w ON
        ST_CONTAINS(t_w.geometry, ST_GEOGPOINT(raw.{long_field}, raw.{lat_field}))
        AND TIMESTAMP(PARSE_DATETIME('%m/%d/%Y %H:%M:%S-00:00',t_w.start_date)) <= TIMESTAMP(raw.{create_date})
        AND IFNULL(TIMESTAMP(PARSE_DATETIME('%m/%d/%Y %H:%M:%S-00:00', t_w.end_date)),
            CURRENT_TIMESTAMP()) >= TIMESTAMP(raw.{create_date})

      -- fire zones
      JOIN `data-rivers.timebound_geography.fire_zones` AS t_fz ON
         ST_CONTAINS(t_fz.geometry, ST_GEOGPOINT(raw.{long_field}, raw.{lat_field}))
        AND TIMESTAMP(PARSE_DATETIME('%m/%d/%Y %H:%M:%S-00:00',t_fz.start_date)) <= TIMESTAMP(raw.{create_date})
        AND IFNULL(TIMESTAMP(PARSE_DATETIME('%m/%d/%Y %H:%M:%S-00:00', t_fz.end_date)),
            CURRENT_TIMESTAMP()) >= TIMESTAMP(raw.{create_date})

      -- police zones
      JOIN `data-rivers.timebound_geography.police_zones` AS t_pz ON
         ST_CONTAINS(t_pz.geometry, ST_GEOGPOINT(raw.{long_field}, raw.{lat_field}))
        AND TIMESTAMP(PARSE_DATETIME('%m/%d/%Y %H:%M:%S-00:00',t_pz.start_date)) <= TIMESTAMP(raw.{create_date})
        AND IFNULL(TIMESTAMP(PARSE_DATETIME('%m/%d/%Y %H:%M:%S-00:00', t_pz.end_date)),
            CURRENT_TIMESTAMP()) >= TIMESTAMP(raw.{create_date})

      -- DPW streets division
      JOIN `data-rivers.timebound_geography.dpw_streets_divisions` AS t_st ON
       ST_CONTAINS(t_st.geometry, ST_GEOGPOINT(raw.{long_field}, raw.{lat_field}))
        AND TIMESTAMP(PARSE_DATETIME('%m/%d/%Y %H:%M:%S-00:00',t_st.start_date)) <= TIMESTAMP(raw.{create_date})
        AND IFNULL(TIMESTAMP(PARSE_DATETIME('%m/%d/%Y %H:%M:%S-00:00', t_st.end_date)),
            CURRENT_TIMESTAMP()) >= TIMESTAMP(raw.{create_date})

      -- DPW environment services division
      JOIN `data-rivers.timebound_geography.dpw_es_divisions` AS t_es ON 
         ST_CONTAINS(t_es.geometry, ST_GEOGPOINT(raw.{long_field}, raw.{lat_field}))
        AND TIMESTAMP(PARSE_DATETIME('%m/%d/%Y %H:%M:%S-00:00',t_es.start_date)) <= TIMESTAMP(raw.{create_date})
        AND IFNULL(TIMESTAMP(PARSE_DATETIME('%m/%d/%Y %H:%M:%S-00:00', t_es.end_date)),
            CURRENT_TIMESTAMP()) >= TIMESTAMP(raw.{create_date})

      -- DPW parks division
      JOIN `data-rivers.timebound_geography.dpw_parks_divisions` AS t_pk ON
         ST_CONTAINS(t_pk.geometry, ST_GEOGPOINT(raw.{long_field}, raw.{lat_field}))
        AND TIMESTAMP(PARSE_DATETIME('%m/%d/%Y %H:%M:%S-00:00',t_pk.start_date)) <= TIMESTAMP(raw.{create_date})
        AND IFNULL(TIMESTAMP(PARSE_DATETIME('%m/%d/%Y %H:%M:%S-00:00', t_pk.end_date)),
            CURRENT_TIMESTAMP()) >= TIMESTAMP(raw.{create_date})
    )

    -- join in the zones that were assigned in sel_zones with ALL of the records (including those that could not be 
    rev coded above)
    SELECT 
        raw.* EXCEPT(neighborhood_name, council_district, ward, fire_zone, police_zone, dpw_streets, dpw_enviro, 
        dpw_parks),
        sel_zones.* EXCEPT (id)
    FROM `{os.environ["GCLOUD_PROJECT"]}.{dataset}.{raw_table}` raw
    LEFT OUTER JOIN sel_zones ON sel_zones.{id_col} = raw.{id_col}
"""

# TODO: this function will be deprecated ASAP (see above function)
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
          fire_zones.firezones AS fire_zone,`
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
        blobs = storage_client.list_blobs(bucket_name, prefix = prefix)
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


def format_dataflow_call(script_name, bucket_name, sub_direc, dataset_id):
    """
        Find the date of the last time a GCS loader was run successfully and use
        that information to format a string containing all necessary runtime arguments for a bash operator
        to execute the dataflow script
        :param script_name: name of dataflow script to execute (string) e.g. "qalert_requests_dataflow.py"
        :param bucket_name: name of GCS bucket (string) e.g. "qalert"
        :param sub_direc: name of directory within bucket_name to be searched (string) e.g. "requests" (located in
        qalert bucket)
        :param dataset_id: name of dataset that is attached to json file from GCS loader script (string) e.g.
        requests (this is sometimes redundant with the sub_direc and is here to allow greater flexibility)
        :return: string containing all bash arguments for execution of script
        """

    exec_script_cmd = f"python {os.environ['DAGS_PATH']}/dependencies/dataflow_scripts/{script_name}"

    # grab the latest GCS upload
    bucket = storage_client.bucket(f"{os.environ['GCS_PREFIX']}_{bucket_name}")
    blob = bucket.get_blob(f"{sub_direc}/successful_run_log/log.json")
    run_info = blob.download_as_string()
    last_run = ndjson.loads(run_info.decode('utf-8'))[0]["current_run"].replace(" ", "_")
    last_run_split = last_run.partition("_")[0].split("-")
    date_direc = f"{last_run_split[0]}/{last_run_split[1]}/{last_run_split[2]}"
    ts = last_run.split("_")[1]

    input_arg = f" --input gs://{os.environ['GCS_PREFIX']}_{bucket_name}/{sub_direc}/{date_direc}/{last_run}_" \
                f"{dataset_id}.json"
    output_arg = f" --avro_output gs://{os.environ['GCS_PREFIX']}_{bucket_name}/{sub_direc}/avro_output/" \
                 f"{date_direc}/{ts}/"
    return exec_script_cmd + input_arg + output_arg


def build_city_limits_query(dataset, raw_table, lat_field = 'lat', long_field = 'long'):
    """
    Determine whether a set of coordinates fall within the borders of the City of Pittsburgh,
    while also falling outside the borders of Mt. Oliver. If an address is within the city,
    the address_type field is left as-is. Otherwise, address_type is changed to 'Outside
    of City'.
    :param dataset: source BigQuery dataset that contains the table to be updated
    :param raw_table: name of the table containing raw 311 data
    :param lat_field: name of table column that contains latitude value
    :param long_field: name of table column that contains longitude value
    :return: string to be passed through as arg to BigQueryOperator
    **NOTE**: A strange issue occurs with the Mt Oliver borders if it is stored in BigQuery in GEOGRAPHY format.
    All lat/longs outside of the Mt Oliver boundaries are identified as inside Mt Oliver when passed through ST_COVERS,
    and all lat/longs inside of Mt Oliver are identified as outside of it. To get around this problem, we have stored
    the boundary polygons as strings, and then convert those strings to polygons using ST_GEOGFROMTEXT.
    """

    return f"""
    UPDATE `{os.environ['GCLOUD_PROJECT']}.{dataset}.{raw_table}`
    SET address_type = IF ( 
       ((ST_COVERS((ST_GEOGFROMTEXT((SELECT geometry FROM `data-rivers.geography.pittsburgh_and_mt_oliver_borders`
                                      WHERE city = 'Mt. Oliver'))),
               ST_GEOGPOINT(`{os.environ['GCLOUD_PROJECT']}.{dataset}.{raw_table}`.{long_field},
                    `{os.environ['GCLOUD_PROJECT']}.{dataset}.{raw_table}`.{lat_field})))
        OR NOT 
        ST_COVERS((ST_GEOGFROMTEXT((SELECT geometry FROM `data-rivers.geography.pittsburgh_and_mt_oliver_borders`
                                     WHERE city = 'Pittsburgh'))),
                   ST_GEOGPOINT(`{os.environ['GCLOUD_PROJECT']}.{dataset}.{raw_table}`.{long_field}, 
                   `{os.environ['GCLOUD_PROJECT']}.{dataset}.{raw_table}`.{lat_field}))
       ), 'Outside of City', address_type )
    WHERE `{os.environ['GCLOUD_PROJECT']}.{dataset}.{raw_table}`.{long_field} IS NOT NULL AND 
    `{os.environ['GCLOUD_PROJECT']}.{dataset}.{raw_table}`.{lat_field} IS NOT NULL
    """


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
