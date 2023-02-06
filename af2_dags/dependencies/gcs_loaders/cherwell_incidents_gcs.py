import os
import argparse
import pendulum
from datetime import datetime, timedelta
import requests
from google.cloud import storage

from gcs_utils import json_to_gcs, find_last_successful_run

storage_client = storage.Client()
bucket = f"{os.environ['GCS_PREFIX']}_cherwell"

parser = argparse.ArgumentParser()
parser.add_argument('--output_arg', dest='out_loc', required=True,
                    help='fully specified location to upload the ndjson file of incident data')
args = vars(parser.parse_args())

yesterday = datetime.combine(datetime.now(tz = pendulum.timezone("utc")) - timedelta(1),
                             datetime.min.time()).strftime("%Y-%m-%d %H:%M:%S")

AUTH_URL = f"{os.environ['CHERWELL_BASE_URL']}/CherwellAPI/token"
auth_body = f"grant_type=password&client_id={os.environ['CHERWELL_API_KEY']}&username={os.environ['CHERWELL_USER']}&" \
            f"password={os.environ['CHERWELL_PW']}"

oauth = requests.post(AUTH_URL, data=auth_body)
token = oauth.json()['access_token']

SEARCH_URL = f"{os.environ['CHERWELL_BASE_URL']}/CherwellAPI/api/V1/getsearchresults"
payload = {
    "association": os.environ['CHERWELL_ASSOC'],
    "SCOPE": "Global",
    "scopeowner": "(None)",
    "searchid": os.environ['CHERWELL_SEARCHID']
}

# find the last successful DAG run (needs to be specified in UTC YYYY-MM-DD HH:MM:SS)
# if there was no previous good run, default to yesterday's date.
# this is used to initialize the payload below
run_start_win, first_run = find_last_successful_run(bucket, "incidents/successful_run_log/log.json", yesterday)

all_records = []
url = SEARCH_URL
# Make use of session to retain authorization header when a URL redirect happens
s = requests.session()
more = True
while more is True:
    # API call to get data
    response = s.post(url, headers={'Content-Type': 'application/json', 'Authorization': f'Bearer {token}'},
                      json=payload)
    curr_run = datetime.now(tz = pendulum.timezone('UTC')).strftime("%Y-%m-%d %H:%M:%S")
    print(f"Response at {str(curr_run)}: {str(response.status_code)}")

    incidents = response.json()['businessObjects']
    # continue looping through records until the API indicates that there are no more left
    if response.json()['hasMoreRecords']:
        next_url_index = next((i for (i, d) in enumerate(response.json()['links']) if d['name']=='Next Page'), None)
        url = response.json()['links'][next_url_index]['url']
    else:
        more = False
    # append list of API results to growing all_records list (should not need more than initial API call)
    all_records += incidents

    # write the successful run information (used by each successive run to find the backfill start date)
    successful_run = {
        "requests_retrieved": len(incidents),
        "since": run_start_win,
        "current_run": curr_run,
        "note": "Data retrieved between the time points listed above"
    }
    json_to_gcs("incidents/successful_run_log/log.json", [successful_run],
                bucket)

json_to_gcs(f"{args['out_loc']}", all_records, bucket)