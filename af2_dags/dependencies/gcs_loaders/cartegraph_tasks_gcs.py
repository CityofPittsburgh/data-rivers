import os
import argparse
import pendulum
from datetime import datetime, timedelta
import requests
from requests.auth import HTTPBasicAuth
from google.cloud import storage

from gcs_utils import json_to_gcs, find_last_successful_run

API_LIMIT = 5000

storage_client = storage.Client()
bucket = f"{os.environ['GCS_PREFIX']}_cartegraph"

parser = argparse.ArgumentParser()
parser.add_argument('--output_arg', dest='out_loc', required=True,
                    help='fully specified location to upload the combined ndjson file')
args = vars(parser.parse_args())

yesterday = datetime.combine(datetime.now(tz = pendulum.timezone("utc")) - timedelta(1),
                             datetime.min.time()).strftime("%Y-%m-%d %H:%M:%S")

BASE_URL = f"https://cgweb06.cartegraphoms.com/PittsburghPA/api/v1/classes/cgTasksClass"

auth = HTTPBasicAuth(os.environ['CARTEGRAPH_USER'], os.environ['CARTEGRAPH_PW'])
sort = 'EntryDateField:asc'

# find the last successful DAG run (needs to be specified in UTC YYYY-MM-DD HH:MM:SS)
# if there was no previous good run, default to yesterday's date.
# this is used to initialize the payload below
offset, first_run = find_last_successful_run(bucket, "tasks/successful_run_log/log.json", 0)

all_records = []
more = True
while more is True:
    payload = {'sort': sort, 'limit': API_LIMIT, 'offset': offset}
    # API call to get data
    response = requests.get(BASE_URL, auth=auth, params=payload)
    curr_run = datetime.now(tz = pendulum.timezone('UTC')).strftime("%Y-%m-%d %H:%M:%S")
    print("Response at " + str(curr_run) + ": " + str(response.status_code))

    tasks = response.json()['cgTasksClass']
    total = int(response.json()['_metadata']['totalCount'])
    diff = total - int(offset)
    # continue looping through records until we have captured the total count of records
    if diff < API_LIMIT:
        more = False
    else:
        offset = int(offset) + API_LIMIT
    # append list of API results to growing all_records list
    all_records += tasks

    # write the successful run information (used by each successive run to find the backfill start date)
    successful_run = {
        "requests_retrieved": len(tasks),
        "since": curr_run,
        "current_run": offset,
        "note": "Final chunk of data retrieved using the offset value listed above"
    }
    json_to_gcs("tasks/successful_run_log/log.json", [successful_run],
                bucket)

json_to_gcs(f"{args['out_loc']}", all_records, bucket)