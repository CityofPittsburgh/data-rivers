import os
import argparse
import time

import requests
import pendulum
import json
from datetime import datetime, timedelta

from gcs_utils import json_to_gcs, replace_pii, find_last_successful_run

# set initial values for loading operation
# parser = argparse.ArgumentParser()
# parser.add_argument('-s', '--since', dest = 'since', required = True,
#                     help = 'Start param for API pull (last successful DAG run as YYYY-MM-DD)')
# parser.add_argument('-e', '--execution_date', dest = 'execution_date',
#                     required = True, help = 'DAG execution date (YYYY-MM-DD)')
# args = vars(parser.parse_args())


bucket = f"{os.environ['GCS_PREFIX']}_qalert"

# find the last successful DAG run (needs to be specified in UTC YYYY-MM-DD HH:MM:SS) if there was no previous good
# run default to yesterday (this does not handle a full self-healing backfill); this is used to initialize the
# payload below
yesterday = datetime.combine(datetime.now(tz = pendulum.timezone("utc")) - timedelta(1),
                             datetime.min.time()).strftime("%Y-%m-%d %H:%M:%S")
last_good_run = find_last_successful_run(bucket, "requests/successful_run_log/log.json", yesterday)

# qscend API requires a value (any value) for the user-agent field
headers = {'User-Agent': 'City of Pittsburgh ETL'}
payload = {'key': os.environ['QALERT_KEY'], 'since': last_good_run}

# continue running the API until data is retrieved (wait 5 min if there is no new data between last_good_run and now (
# curr_run))
data_retrieved = False
while data_retrieved is False:
    # API call to get data
    response = requests.get('https://pittsburghpa.qscend.com/qalert/api/v1/requests/changes', params = payload,
                            headers = headers)
    curr_run = datetime.now(tz = pendulum.timezone('UTC')).strftime("%Y-%m-%d %H:%M:%S")

    # convert the utf-8 encoded string to json
    full_requests = response.json()['request']

    # verify the API called returned data (if no new tickets, then type will be NONE)
    if full_requests is not None:
        data_retrieved = True
        # write the successful run information (used by each successive DAG run to find the backfill date)
        successful_run = [{"requests_retrieved": len(full_requests),
                          "since"             : last_good_run,
                          "current_run"       : curr_run,
                          "note"              : "Data retrieved between the time points listed above"}]
        json_to_gcs("requests/successful_run_log/log.json", successful_run,
                    bucket)

    else:
        print("No new requests. Sleeping with retry scheduled.")
        time.sleep(300)

# Comments must be scrubbed for PII from 311 requests.
# Comments do not follow strict formatting so this is an imperfect approximation.
# Steps: extract fields, detect person names that are followed by hotwords for exclusion (e.g. park or street),
# place an underscore between the detected words to prevent accidental redaction, redact PII the user information in
# activities needs to be scrubbed, but does not need to be screened for hotwords
pre_clean = {"req_comments": []}
for row in full_requests:
    pre_clean["req_comments"].append(row.get("comments", ""))

# convert list to string that is seperated with a delimiter sequence (allows full list to be processed as one string,
# as opposed to mapping it to each element which is 100x slower)
delim_seq = os.environ["DELIM"]
pre_clean["req_comments"] = delim_seq.join(pre_clean["req_comments"])

# scrub pii
scrubbed_req_comments = replace_pii(pre_clean["req_comments"], retain_location = True)

# convert delim-seperated string back to list of strings
split_req_comments = scrubbed_req_comments.split(delim_seq.strip())

# overwrite the original fields with scrubbed data
for i in range(len(full_requests)):
    full_requests[i]["comments"] = split_req_comments[i].strip()

# load to gcs
target_direc = "requests"
year = curr_run.split('-')[0]
month = curr_run.split('-')[1]
day = curr_run.split('-')[2].split(' ')[0]
mod_date_time = curr_run.replace(" ","_")
target_path = f"{target_direc}/{year}/{month}/{day}/{mod_date_time}_requests.json"
json_to_gcs(target_path, full_requests, bucket)

# Write a last update val to that sub direc
