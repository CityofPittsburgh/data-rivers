import os
import argparse
import pendulum
from datetime import datetime, timedelta
import requests
from urllib.request import urlopen
from requests.auth import HTTPBasicAuth
from google.cloud import storage

from gcs_utils import json_to_gcs, find_last_successful_run

API_LIMIT = 5000

storage_client = storage.Client()
bucket = f"{os.environ['GCS_PREFIX']}_ceridian"

parser = argparse.ArgumentParser()
parser.add_argument('--output_arg', dest='out_loc', required=True,
                    help='fully specified location to upload the combined ndjson file')
args = vars(parser.parse_args())

yesterday = datetime.combine(datetime.now(tz = pendulum.timezone("utc")) - timedelta(1),
                             datetime.min.time()).strftime("%Y-%m-%d %H:%M:%S")

BASE_URL = f"https://us62e2-services.dayforcehcm.com/Api/{os.environ['CERIDIAN_ORG_ID']}/V1/Reports/APIINFO?pageSize=5000"

auth = HTTPBasicAuth(os.environ['CERIDIAN_USER'], os.environ['CERIDIAN_PW'])
payload = {'pageSize': API_LIMIT}

# find the last successful DAG run (needs to be specified in UTC YYYY-MM-DD HH:MM:SS)
# if there was no previous good run, default to yesterday's date.
# this is used to initialize the payload below
run_start_win, first_run = find_last_successful_run(bucket, "employees/successful_run_log/log.json", yesterday)

all_records = []
url = BASE_URL
more = True
while more is True:
    # API call to get data
    response = requests.get(url, auth=auth, params=payload)
    curr_run = datetime.now(tz = pendulum.timezone('UTC')).strftime("%Y-%m-%d %H:%M:%S")
    print("Response at " + str(curr_run) + ": " + str(response.status_code))

    employees = response.json()['Data']['Rows']
    # continue looping through records until the API has a MoreFlag value of 0
    if response['Paging']['Next']:
        url = response['Paging']['Next']
    else:
        more = False
    # append list of API results to growing all_records list (should not need more than initial API call)
    all_records += employees

    # write the successful run information (used by each successive run to find the backfill start date)
    successful_run = {
        "requests_retrieved": len(employees),
        "since": run_start_win,
        "current_run": curr_run,
        "note": "Data retrieved between the time points listed above"
    }
    json_to_gcs("energy/successful_run_log/log.json", [successful_run],
                bucket)

json_to_gcs(f"{args['out_loc']}", all_records, bucket)