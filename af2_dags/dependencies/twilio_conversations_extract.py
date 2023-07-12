import os
import requests
import io
import json
import time
import pandas as pd
from datetime import datetime
from google.cloud import storage
from gcs_loaders.gcs_utils import find_last_successful_run, json_to_gcs, conv_avsc_to_bq_schema
from dataflow_scripts.dataflow_utils.pandas_utils import change_data_type, df_to_partitioned_bq_table, \
    set_col_b_based_on_col_a_val, swap_two_columns


storage_client = storage.Client()
json_bucket = f"{os.environ['GCS_PREFIX']}_twilio"
BASE_URL = 'https://analytics.ytica.com'
FINAL_COLS = ['id', 'date_time', 'day_of_week', 'agent', 'customer_phone', 'kind', 'direction', 'wait_time',
              'talk_time', 'wrap_up_time', 'hold_time']
BASE_HEADERS = {'Accept': 'application/json', 'Content-Type': 'application/json'}
today = datetime.today()


def get_temp_token(url, headers, bucket):
    token_bucket = storage_client.bucket(bucket)
    token_blob = token_bucket.get_blob(os.environ['FLEX_INSIGHTS_TOKEN_PATH'])
    token_text = token_blob.download_as_string()
    sst = json.loads(token_text.decode('utf-8'))['userLogin']['token']

    sst_headers = headers.copy()
    sst_headers['X-GDC-AuthSST'] = sst
    token_req = requests.get(f"{url}/gdc/account/token", headers=sst_headers)
    print("Temporary token response code: " + str(token_req.status_code))
    tt = token_req.json()['userToken']['token']
    tt_headers = headers.copy()
    tt_headers.update({'Cookie': f"GDCAuthTT={tt}"})
    return tt_headers


run_start_win, first_run = find_last_successful_run(json_bucket, "conversations/successful_run_log/log.json",
                                                    "2020-04-03")

# Super Secure Tokens (SSTs) last for two weeks, so this code checks to see if a SST has been uploaded within that time
# window. If it has, it is pulled from GCS and used in the following API requests. If not, a new one is generated
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
        auth = requests.post(f"{BASE_URL}/gdc/account/login", data=auth_body, headers=BASE_HEADERS)
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

token_headers = get_temp_token(BASE_URL, BASE_HEADERS, json_bucket)
report_body = F"""
\u007b
    "report_req": \u007b
        "report": "/gdc/md/{os.environ['TWILIO_SD_WORKSPACE']}/obj/{os.environ['FLEX_INSIGHTS_REPORT_ID']}"
    \u007d
\u007d"""
report_req = requests.post(f"{BASE_URL}/gdc/app/projects/{os.environ['TWILIO_SD_WORKSPACE']}/execute/raw",
                           data=report_body, headers=token_headers)
print("Flex Insights report response code: " + str(report_req.status_code))
uri = report_req.json()['uri']

export_status = '202'
round = 1
attempt = 1
delay = 3

# From https://www.twilio.com/docs/flex/developer/insights/api/export-data:
#       "If you receive 202 response from API, it means that the request is accepted, but not ready to be delivered
#       (still computing or preparing the CSV). Since the export API can take several seconds, or minutes in edge cases,
#       depending on the volume of data and number of columns, you need to add a retry in case the server returns 202.
#       The data is only ready to download when the server returns 200."
# This loop repeats until an API returns a 200 response, waiting progressively longer and longer before each request.
# However, since the temporary token only lasts 10 minutes, a new token is retrieved whenever 401 response occurs
# (indicating the token has expired). The loop terminates after 5 rounds of refreshed tokens.
while export_status == '202' and round <= 5:
    export_req = requests.get(f"{BASE_URL}{uri}", headers=token_headers)
    export_status = str(export_req.status_code)
    print(f"Flex Insights export response code on round {round}, attempt #{attempt}: {export_status}")
    if export_status == '200':
        df = pd.read_csv(io.BytesIO(export_req.content), encoding='ascii', parse_dates=[['Date', 'Time']])
        break
    elif export_status == '401':
        token_headers = get_temp_token(BASE_URL, BASE_HEADERS, json_bucket)
        export_status = '202'
        round += 1
        attempt = 1
        delay = 3
    else:
        attempt += 1
        if attempt >= 5:
            time.sleep(delay)
            delay *= 1.25

# mark abandoned calls as a 'kind' of conversation, drop 'abandoned' column afterward
df['Kind'] = df.apply(set_col_b_based_on_col_a_val, col_a='Abandoned', col_b='Kind', check_val='Yes',
                      new_val='Abandoned Conversation', axis=1)
df = df.drop('Abandoned', axis=1)

# convert phone numbers to string
df = change_data_type(df, {'Customer Phone': str})

# convert all different Null types to a single type (None)
df = df.applymap(lambda x: None if isinstance(x, str) and x == '' else x)
df = df.where(df.notnull(), None)

# reorder and rename columns
df = swap_two_columns(df, 'Date_Time', 'Segment')
df.columns = FINAL_COLS

#  read in AVRO schema and load into BQ
schema = conv_avsc_to_bq_schema(F"{os.environ['GCS_PREFIX']}_avro_schemas", "twilio_conversations.avsc")
df_to_partitioned_bq_table(df, 'twilio', 'incoming_conversations', schema, 'MONTH', 'WRITE_TRUNCATE')
