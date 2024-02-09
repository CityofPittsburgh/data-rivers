from __future__ import absolute_import

import logging
import os
import urllib
import json
import ndjson
import math
import pendulum
import base64

from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Attachment, FileContent, FileName, FileType, Disposition, Personalization, Cc, To
from datetime import datetime, timedelta, date
from google.cloud import bigquery, storage
from google.api_core.exceptions import NotFound

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


# TODO: email can be added in later, but that functionality is currently not used. expect this to change soon (05/22)
# 'email': os.environ['EMAIL'],
default_args = {
    'depends_on_past': False,
    'start_date': yesterday,
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


def get_ds_day(prev_ds):
    return prev_ds.split('-')[2]


def get_prev_ds_year(prev_ds):
    return prev_ds.split('-')[0]


def get_prev_ds_month(prev_ds):
    return prev_ds.split('-')[1]


def get_prev_ds_day(ds):
    return ds.split('-')[2]


def build_piecemeal_revgeo_query(dataset, raw_table, new_table, create_date, id_col, lat_field, long_field,
                                 geo_table, geo_field, table_or_view='TABLE'):
    return f"""
    CREATE OR REPLACE {table_or_view} `{os.environ["GCLOUD_PROJECT"]}.{dataset}.{new_table}` AS

    WITH
      sel_zones AS (
      SELECT DISTINCT
        raw.{id_col},
        CAST (geo.zone AS STRING) AS {geo_field}
      FROM
        `{os.environ["GCLOUD_PROJECT"]}.{dataset}.{raw_table}` raw

      LEFT OUTER JOIN `{os.environ["GCLOUD_PROJECT"]}.timebound_geography.{geo_table}` AS geo ON
        ST_CONTAINS(geo.geometry, ST_GEOGPOINT(raw.{long_field}, raw.{lat_field}))
        AND TIMESTAMP(PARSE_DATETIME('%m/%d/%Y %H:%M:%S-00:00',geo.start_date)) <= TIMESTAMP(raw.{create_date})
        AND IFNULL(TIMESTAMP(PARSE_DATETIME('%m/%d/%Y %H:%M:%S-00:00', geo.end_date)),
            CURRENT_TIMESTAMP()) >= TIMESTAMP(raw.{create_date})
    )

    -- join in the zones that were assigned in sel_zones with ALL of the records (including those that could not be 
    -- rev coded above)
    SELECT DISTINCT
        raw.* EXCEPT({geo_field}),
        sel_zones.* EXCEPT ({id_col})
    FROM `{os.environ["GCLOUD_PROJECT"]}.{dataset}.{raw_table}` raw
    LEFT OUTER JOIN sel_zones ON sel_zones.{id_col} = raw.{id_col};
    """


def build_split_table_query(dataset, raw_table, start, stop, num_shards, date_field, cols_in_order):
    query = ""
    step = math.ceil((stop - start) / num_shards)
    timestamps = range(start, stop, step)

    for i in range(len(timestamps)):
        query += F"""CREATE OR REPLACE TABLE `{os.environ['GCLOUD_PROJECT']}.{dataset}.{raw_table}_{i + 1}` AS
        WITH formatted  AS 
            (
            SELECT 
                DISTINCT * EXCEPT (pii_lat, pii_long, anon_lat, anon_long),
                CAST(pii_lat AS FLOAT64) AS pii_lat,
                CAST(pii_long AS FLOAT64) AS pii_long,
                CAST(anon_lat AS FLOAT64) AS anon_lat,
                CAST(anon_long AS FLOAT64) AS anon_long
            FROM 
                `{os.environ['GCLOUD_PROJECT']}.{dataset}.{raw_table}` """
        if i == 0:
            query += f"WHERE {date_field} < {timestamps[i + 1]}"
        elif i == len(timestamps) - 1:
            query += f"WHERE {date_field} >= {timestamps[i]}"
        else:
            query += f"WHERE {date_field} >= {timestamps[i]} AND {date_field} < {timestamps[i + 1]}"
        query += F""")
        SELECT 
            {cols_in_order} 
        FROM 
            formatted;
        """

    return query


def create_partitioned_bq_table(avro_bucket, schema_name, table_id, partition):
    # bigquery schemas that are used to upload directly from pandas are not formatted identically as an avsc filie.
    # this func makes the necessary conversions. this allows a single schema to serve both purposes

    blob = storage.Blob(name = schema_name, bucket = storage_client.get_bucket(avro_bucket))
    schema_text = blob.download_as_string()
    schema = json.loads(schema_text)
    schema = schema['fields']

    change_look_up = {"float": "FLOAT64", "integer": "INT64", "boolean": "BOOL"}

    schema_lst = []
    for s in schema:
        f = s["name"]

        if 'null' in s["type"]:
            s["type"].remove('null')
        curr_type = s["type"][0]

        if curr_type in change_look_up.keys(): curr_type = change_look_up[curr_type]
        t = curr_type.upper()

        schema_lst.append(bigquery.SchemaField(f, t))

    table = bigquery.Table(table_id, schema = schema_lst)
    table.time_partitioning = bigquery.table.TimePartitioning(type_ = partition)

    bq_client.create_table(table)


def gcs_to_email(bucket, file_path, recipients, cc, subject, message, attachment_name, on_certain_day=(False, None),
                 file_type='csv', min_length=50,  **kwargs):
    if (not on_certain_day[0]) or (on_certain_day[0] and date.today().weekday() == on_certain_day[1]):
        try:
            bucket = storage_client.get_bucket(bucket)
            blob = bucket.blob(file_path)
            content = blob.download_as_string()
            if len(content) >= min_length:
                message = Mail(
                    from_email=os.environ['EMAIL'],
                    subject=subject,
                    html_content=F"""
                                {message}
                                """
                )
                recips = Personalization()
                for addr in recipients:
                    recips.add_to(To(addr))
                if cc:
                    for addr in cc:
                        recips.add_cc(Cc(addr))
                message.add_personalization(recips)

                encoded_file = base64.b64encode(content).decode()

                attached_file = Attachment(
                    FileContent(encoded_file),
                    FileName(f"{attachment_name}.{file_type}"),
                    FileType(f'application/{file_type}'),
                    Disposition('attachment')
                )
                message.attachment = attached_file

                if json.loads(os.environ['USE_PROD_RESOURCES'].lower()):
                    sg = SendGridAPIClient(os.environ['SENDGRID_API_KEY'])
                    response = sg.send(message)
                else:
                    print(f'Test complete, following email not sent: \n{message}')
            else:
                print('Requested file is empty, no email sent')
        except NotFound:
            print('Requested file not found')
    else:
        print(f'No email sent, comparison only performed on day of week {on_certain_day[1]}')


def beam_cleanup_statement(bucket):
    return "if gsutil -q stat gs://{}/beam_output/*; then gsutil rm gs://{}/beam_output/**; else echo " \
           "no beam output; fi".format(bucket, bucket)


def check_blob_exists(bucket, path, **kwargs):
    for _ in storage_client.list_blobs(bucket, prefix=path):
        return True
    return False


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


def log_task(dag_id, message, **kwargs):
    print(f'Logging DAG {dag_id}: \n{message}')


def perform_data_quality_check(file_name, **kwargs):
    try:
        bucket = storage_client.get_bucket(f"{os.environ['GCS_PREFIX']}_data_quality_check")
        new_blob = bucket.blob(f"TEMP_{file_name}")
        new_content = new_blob.download_as_string()
        new_dq = ndjson.loads(new_content)

        try:
            old_blob = bucket.blob(file_name)
            old_content = old_blob.download_as_string()
            old_dq = ndjson.loads(old_content)

            msg_content = f"<p>Previously untracked value(s) detected in reference file <i>{file_name}</i>:<br/><ul>"
            uncaught = []
            for item in new_dq:
                val = next(iter(item.items()))[1]
                if item not in old_dq and val:
                    print(f"Untracked value: {item}")
                    msg_content += f"<li>{val}</li>"
                    uncaught.append(val)

            if uncaught:
                msg_content += f"</ul>Check the Airflow logs and contact data stewards for more info."
                send_team_email_notification("Data Quality Notification", msg_content)

            old_blob.delete()
            bucket.copy_blob(new_blob, bucket, file_name)
            bucket.delete_blob(f"TEMP_{file_name}")

        except NotFound as nf:
            print(nf)
            print('First DAG run with data quality checking - generating reference file now')
            bucket.copy_blob(new_blob, bucket, file_name)
            bucket.delete_blob(f"TEMP_{file_name}")

    except NotFound as nf:
        print(nf)
        print('Data quality reference file not found in GCS')


def send_team_email_notification(subject, message_contents):
    message = Mail(
        from_email=os.environ['EMAIL'],
        to_emails=os.environ['EMAIL'],
        subject=subject,
        html_content=message_contents
    )
    sg = SendGridAPIClient(os.environ['SENDGRID_API_KEY'])
    response = sg.send(message)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
