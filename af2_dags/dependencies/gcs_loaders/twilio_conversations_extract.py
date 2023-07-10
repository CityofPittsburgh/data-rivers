import os
import requests
import io
import json
import time
import pandas as pd
from datetime import datetime
from google.cloud import storage
from gcs_utils import find_last_successful_run, json_to_gcs, conv_avsc_to_bq_schema
from af2_dags.dependencies.dataflow_scripts.dataflow_utils.pandas_utils import swap_two_columns,\
    df_to_partitioned_bq_table

storage_client = storage.Client()
json_bucket = f"{os.environ['GCS_PREFIX']}_twilio"
BASE_URL = 'https://analytics.ytica.com'

FINAL_COLS = ['id', 'date_time', 'day_of_week', 'agent', 'external_contact', 'abandoned', 'kind', 'direction',
              'talk_time', 'wait_time', 'wrap_up_time']

today = datetime.today()

run_start_win, first_run = find_last_successful_run(json_bucket, "conversations/successful_run_log/log.json",
                                                    "2020-04-03")

auth_headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
if (today - datetime.strptime(run_start_win, '%Y-%m-%d')).days >= 14:
    auth_body = F"""
    \u007b
        "postUserLogin":\u007b
            "login":"{os.environ['FLEX_INSIGHTS_UN']}",
            "password":"{os.environ['FLEX_INSIGHTS_PW']}",
            "remember": 0,
            "verify_level": 2
        \u007d
    \u007d"""

    auth_status = ''
    attempt = 1
    curr_delay = 1
    max_delay = 10
    while auth_status != '200':
        auth = requests.post(f"{BASE_URL}/gdc/account/login", data=auth_body, headers=auth_headers)
        auth_status = str(auth.status_code)
        if auth_status == '200':
            print("New SST Token Retrieved")
            json_to_gcs(f"{os.environ['FLEX_INSIGHTS_TOKEN_PATH']}", [auth.json()], json_bucket)
            successful_run = [{"since": run_start_win,
                               "current_run": today.strftime("%Y-%m-%d"),
                               "note": "Super Secure Token retrieved between the time points listed above"}]
            json_to_gcs("conversations/successful_run_log/log.json", successful_run, json_bucket)
        else:
            time.sleep(curr_delay)
            curr_delay *= 3

            # all other conditions trigger break and are noted in data
            if curr_delay > max_delay:
                print(f"Flex Insights authentication failed after {attempt} attempts")
                break

            # increment count for reporting
            attempt += 1

token_bucket = storage_client.bucket(json_bucket)
token_blob = token_bucket.get_blob(os.environ['FLEX_INSIGHTS_TOKEN_PATH'])
token_text = token_blob.download_as_string()
sst = json.loads(token_text.decode('utf-8'))['userLogin']['token']

token_headers = auth_headers.copy()
token_headers['X-GDC-AuthSST'] = sst
token_req = requests.get(f"{BASE_URL}/gdc/account/token", headers=token_headers)
print("Temporary token response code: " + str(token_req.status_code))
tt = token_req.json()['userToken']['token']

report_headers = auth_headers.copy()
cookie = {'Cookie': f"GDCAuthTT={tt}"}
report_headers.update(cookie)
report_body = F"""
\u007b
    "report_req": \u007b
        "report": "/gdc/md/{os.environ['TWILIO_SD_WORKSPACE']}/obj/{os.environ['FLEX_INSIGHTS_REPORT_ID']}"
    \u007d
\u007d"""
report_req = requests.post(f"{BASE_URL}/gdc/app/projects/{os.environ['TWILIO_SD_WORKSPACE']}/execute/raw",
                           data=report_body, headers=report_headers)
print("Flex Insights report response code: " + str(report_req.status_code))
uri = report_req.json()['uri']

export_status = '202'
attempt = 1
delay = 3

while export_status == '202' and attempt <= 25:
    export_req = requests.get(f"{BASE_URL}{uri}", headers=cookie)
    export_status = str(export_req.status_code)
    print(f"Flex Insights export response code on attempt #{attempt}: {export_status}")
    if export_status == '200':
        df = pd.read_csv(io.BytesIO(export_req.content), encoding='ascii', parse_dates=[['Date', 'Time']])
        break
    else:
        attempt += 1
        if attempt >= 5:
            time.sleep(delay)
            delay *= 1.25

# convert all different Null types to a single type (None)
df = df.applymap(lambda x: None if isinstance(x, str) and x == '' else x)
df = df.where(df.notnull(), None)
df = swap_two_columns(df, 'Date_Time', 'Segment')
df.columns = FINAL_COLS

#  read in AVRO schema and load into BQ
schema = conv_avsc_to_bq_schema(F"{os.environ['GCS_PREFIX']}_avro_schemas", "twilio_conversations.avsc")
df_to_partitioned_bq_table(df, 'twilio', 'flex_insights_conversations', schema, 'MONTH', 'WRITE_TRUNCATE')
