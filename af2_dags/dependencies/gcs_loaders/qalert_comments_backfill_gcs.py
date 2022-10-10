import os

import requests
from datetime import datetime, timedelta, date
import numpy as np
from math import ceil
from google.cloud import storage

from gcs_utils import json_to_gcs, find_last_successful_run

today = date.today()

# set the limit for how many records can be simulataneously sent to the Google DLP API
API_LIMIT = 2000
# init the backfill constants (qalert system went online 4/20/15)
DEFAULT_RUN_START = "2015-04-19 00:00:00"
BACKFILL_STOP = today.strftime("%Y-%m-%d %H:%M:%S")
INCREMENT_DAYS = 14

# init the GCS client for compose operation (to append each batch of data to a growing json)
storage_client = storage.Client()
bucket = f"{os.environ['GCS_PREFIX']}_qalert"

# init run_stop_win to begin the while loop below (stop value begins the same as the start value, which guarantees
# the loop will initiate at least 1 time)
run_stop_win = "2015-04-19 00:00:00"

# run the API calls until all data is retrieved (all API call and data pushes to GCS executed in this loop)
while datetime.strptime(run_stop_win, "%Y-%m-%d %H:%M:%S") <= datetime.strptime(BACKFILL_STOP, "%Y-%m-%d %H:%M:%S"):

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

    # each run of data extracted from the API will be appended to a growing JSON and saved as an individual JSON
    append_target_path = f"requests/backfill/{BACKFILL_STOP.split(' ')[0]}/backfilled_requests.json"
    curr_run_target_path = f"requests/backfill/{BACKFILL_STOP.split(' ')[0]}/{run_stop_win.split(' ')[0]}.json"
    temp_target_path = f"requests/backfill/{BACKFILL_STOP.split(' ')[0]}/temp_uploaded_blob.json"

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