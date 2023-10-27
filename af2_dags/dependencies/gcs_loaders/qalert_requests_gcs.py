import os
import time
import argparse

import requests
import pendulum
from datetime import datetime, timedelta
from json.decoder import JSONDecodeError

from gcs_utils import json_to_gcs, find_last_successful_run

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
    response = requests.get('https://pittsburghpa.qscend.com/qalert/api/v1/requests/changes', params = payload,
                            headers = headers)
    curr_run = datetime.now(tz = pendulum.timezone('UTC')).strftime("%Y-%m-%d %H:%M:%S")
    print("Response at " + str(curr_run) + ": " + str(response.status_code))

    try:
        # convert the utf-8 encoded string to json
        full_requests = response.json()['request']

        # verify the API called returned data (if no new tickets, then type will be NONE)
        if full_requests is not None:
            data_retrieved = True

        else:
            print("No new requests. Sleeping with retry scheduled.")
            time.sleep(300)
    except JSONDecodeError:
        print('Too many requests made in a short amount of time. Sleeping for 1 minute')
        time.sleep(60)


# write the successful run information (used by each successive DAG run to find the backfill date)
successful_run = [{"requests_retrieved": len(full_requests),
                   "since": run_start_win,
                   "current_run": curr_run,
                   "destination_file": args["out_loc"],
                   "note": "Data retrieved between the time points listed above"}]
json_to_gcs("requests/successful_run_log/log.json", successful_run,
            bucket)

# load to gcs
json_to_gcs(args["out_loc"], full_requests, bucket)
