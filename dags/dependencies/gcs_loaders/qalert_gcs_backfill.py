import os

import requests
from datetime import datetime, timedelta
import numpy as np
from math import ceil
from google.cloud import storage

from gcs_utils import json_to_gcs, replace_pii, find_last_successful_run

# set the limit for how many records can be simulataneously sent to the Google DLP API
API_LIMIT = 2000
# init the backfill constants (qalert system went online 4/20/15)
DEFAULT_RUN_START = "2015-04-19 00:00:00"
BACKFILL_STOP = datetime.strptime("2022-01-30 00:00:00", "%Y-%m-%d %H:%M:%S")
INCREMENT_DAYS = 14

# init the GCS client for compose operation (to append each batch of data to a growing json)
storage_client = storage.Client()
bucket = f"{os.environ['GCS_PREFIX']}_qalert"

# init run_stop_win to begin the while loop below (stop value begins the same as the start value, which guarantees
# the loop will initiate at least 1 time)
run_stop_win = "2015-04-19 00:00:00"

# run the API calls until all data is retrieved (all API call and data pushes to GCS executed in this loop)
while datetime.strptime(run_stop_win, "%Y-%m-%d %H:%M:%S") <= BACKFILL_STOP:

    # find the last successful DAG run (needs to be specified in UTC YYYY-MM-DD HH:MM:SS); this is used to initialize
    # the payload below; if the Backfill DAG hasn't been run yet, then use the DEFAULT_RUN_START; Increment each pull
    # window by 2 weeks;  the last date to use for the backfill is specified by BACKFILL_STOP
    run_start_win, first_run = find_last_successful_run(bucket, "requests/backfill/successful_run_log/log.json",
                                                        DEFAULT_RUN_START)

    # run the api call until records are returned (full requests != None); increment the stop window each loop
    full_requests = None
    while full_requests is None:
        # increment the run stop window (begins the same as the start window). This line will repeat if no records
        # are returned and the stop window will expand
        run_stop_win = str(datetime.strptime(run_start_win, "%Y-%m-%d %H:%M:%S") + timedelta(days = INCREMENT_DAYS))

        # init qscend API vars (requires a value (any value) for the user-agent field)
        headers = {'User-Agent': 'City of Pittsburgh ETL'}
        payload = {'key': os.environ['QALERT_KEY'], 'start': run_start_win, 'end': run_stop_win}

        # API call
        response = requests.get('https://pittsburghpa.qscend.com/qalert/api/v1/requests/dump', params = payload,
                                headers = headers)

        # convert the utf-8 encoded string to json
        full_requests = response.json()['request']


    # write the successful run information (used by each successive run to find the backfill start date)
    successful_run = {
            "requests_retrieved": len(full_requests),
            "since"             : run_start_win,
            "current_run"       : run_stop_win,
            "note"              : "Data retrieved between the time points listed above"
    }
    json_to_gcs("requests/backfill/successful_run_log/log.json", [successful_run],
                bucket)

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

    # each run of data extracted from the API will be appended to a growing JSON and saved as an individual JSON
    append_target_path = "requests/backfill/2022-01-28/backfilled_requests.json"
    curr_run_target_path = f"requests/backfill/2022-01-28/{run_stop_win.split(' ')[0]}.json"
    temp_target_path = "requests/backfill/2022-01-28/temp_uploaded_blob.json"

    # load each run's data as a unique file
    json_to_gcs(curr_run_target_path, full_requests, bucket)
    if first_run:
        # load the initial run that will be appended
        json_to_gcs(append_target_path, full_requests, bucket)
    else:
        # load the current run's data to gcs as a temp blob to be appended next
        json_to_gcs(temp_target_path, full_requests, bucket)

        # append temp_uploaded_blob.json to a growing json of all data in backfill. As of 11/2021 GCP will not combine
        # more than 32 files, so this operation is performed inside the loop. If fewer files were created overall,
        # this operation could be moved outside the loop.
        bucket_obj = storage_client.bucket(bucket)
        output_file_blob = bucket_obj.blob(append_target_path)
        output_file_blob.content_type = 'application/ndjson'
        output_file_blob.compose([bucket_obj.get_blob(append_target_path), bucket_obj.get_blob(temp_target_path)])