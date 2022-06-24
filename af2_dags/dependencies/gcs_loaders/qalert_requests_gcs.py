import os
import time
import argparse

import requests
import pendulum
from datetime import datetime, timedelta
import numpy as np
from math import ceil

from gcs_utils import json_to_gcs, replace_pii, find_last_successful_run

API_LIMIT = 2000

bucket = f"{os.environ['GCS_PREFIX']}_qalert"

parser = argparse.ArgumentParser()
parser.add_argument('--output_arg', dest='out_loc', required=True,
                    help='fully specified location to upload the ndjson file')
args = vars(parser.parse_args())


# find the last successful DAG run (needs to be specified in UTC YYYY-MM-DD HH:MM:SS) if there was no previous good
# run default to yesterday (this does not handle a full self-healing backfill); this is used to initialize the
# payload below
yesterday = datetime.combine(datetime.now(tz = pendulum.timezone("utc")) - timedelta(1),
                             datetime.min.time()).strftime("%Y-%m-%d %H:%M:%S")
run_start_win, first_run = find_last_successful_run(bucket, "requests/successful_run_log/log.json", yesterday)


# qscend API requires a value (any value) for the user-agent field
headers = {'User-Agent': 'City of Pittsburgh ETL'}
payload = {'key': os.environ['QALERT_KEY'], 'since': run_start_win}


# continue running the API until data is retrieved (wait 5 min if there is no new data between last_good_run and now (
# curr_run))
data_retrieved = False
while data_retrieved is False:
    # API call to get data
    print("Yesterday: " + str(yesterday) + "\nRun Start Window: " + str(run_start_win) + "\nFirst Run?: " + str(first_run) + "\nBucket: " + bucket + "\nHeaders: " + str(headers))
    response = requests.get('https://pittsburghpa.qscend.com/qalert/api/v1/requests/changes', params = payload,
                            headers = headers)
    curr_run = datetime.now(tz = pendulum.timezone('UTC')).strftime("%Y-%m-%d %H:%M:%S")
    print("Response at " + str(curr_run) + ": " + str(response.status_code))

    # convert the utf-8 encoded string to json
    full_requests = response.json()['request']

    # verify the API called returned data (if no new tickets, then type will be NONE)
    if full_requests is not None:
        data_retrieved = True
        # write the successful run information (used by each successive DAG run to find the backfill date)
        successful_run = [{"requests_retrieved": len(full_requests),
                          "since"             : run_start_win,
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
# place an underscore between the detected words to prevent accidental redaction, redact PII
pre_clean = {"req_comments": []}
for row in full_requests:
    pre_clean["req_comments"].append(row.get("comments", ""))


# The Google data loss prevention (dlp) API is used (via helper function) to scrub PII. This API cannot handle
# large payloads. We therefore chop the data into batches to prevent overloading the API. As of 11/2021 we use a
# limit of 2000, but this can be modified by changing the 'API_LIMIT' constant.

# determine the number of bins needed for each batch of comments (round up in case there is a remainder from the
# division); split into batches
bins = ceil(len(pre_clean["req_comments"]) / API_LIMIT)
pre_clean_batches = np.array_split(pre_clean["req_comments"], bins)

delim_seq = os.environ["DELIM"]
all_comms = []
for b in pre_clean_batches:
    # convert list to string that is separated with a delimiter sequence (allows full list to be processed as one string,
    # as opposed to mapping it to each element which is 100x slower)
    batch = delim_seq.join(b)

    # scrub pii
    scrubbed_req_comments = replace_pii(batch, retain_location = True)

    # convert delim-seperated string back to list of strings
    split_req_comments = scrubbed_req_comments.split(delim_seq.strip())

    # extend growing list with scrubbed comments
    all_comms.extend(split_req_comments)


# overwrite the original fields with scrubbed data
for i in range(len(full_requests)):
    full_requests[i]["comments"] = all_comms[i].strip()


# load to gcs
json_to_gcs(args["out_loc"], full_requests, bucket)