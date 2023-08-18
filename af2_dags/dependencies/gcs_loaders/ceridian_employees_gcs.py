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
bucket = f"{os.environ['GCS_PREFIX']}_ceridian"

parser = argparse.ArgumentParser()
parser.add_argument('--output_arg', dest='out_loc', required=True,
                    help='fully specified location to upload the combined ndjson file')
args = vars(parser.parse_args())

yesterday = datetime.combine(datetime.now(tz = pendulum.timezone("utc")) - timedelta(1),
                             datetime.min.time()).strftime("%Y-%m-%d %H:%M:%S")

BASE_URL = f"https://www.dayforcehcm.com/Api/{os.environ['CERIDIAN_ORG_ID']}/V1/Reports/APIINFO?pageSize={API_LIMIT}"
auth = HTTPBasicAuth(os.environ['CERIDIAN_USER'], os.environ['CERIDIAN_PW'])

# find the last successful DAG run (needs to be specified in UTC YYYY-MM-DD HH:MM:SS)
# if there was no previous good run, default to yesterday's date.
# this is used to initialize the payload below
run_start_win, first_run = find_last_successful_run(bucket, "employees/successful_run_log/log.json", yesterday)

all_records = []
url = BASE_URL
# Make use of session to retain authorization header when a URL redirect happens
s = requests.session()
more = True
while more is True:
    # API call to get data
    response = s.get(url, auth=auth)
    # Initial API should fail due to URL change, response gives us new URL
    if response.status_code == 401:
        redir_url = response.url
        # Retry API request with same parameters but new URL
        response = s.get(redir_url, auth=auth)
    curr_run = datetime.now(tz = pendulum.timezone('UTC')).strftime("%Y-%m-%d %H:%M:%S")
    print(f"Response at {str(curr_run)}: {str(response.status_code)}")

    employees = response.json()['Data']['Rows']
    # continue looping through records until the API has a MoreFlag value of 0
    if response.json()['Paging']['Next']:
        url = response.json()['Paging']['Next']
    else:
        more = False
    # append list of API results to growing all_records list (should not need more than initial API call)
    all_records += employees

# exclude records that were made for testing purposes
all_records = [r for r in all_records if r.get('Employee_LastName') != 'Test']

# write the successful run information (used by each successive run to find the backfill start date)
successful_run = {
    "requests_retrieved": len(employees),
    "since": run_start_win,
    "current_run": curr_run,
    "note": "Data retrieved between the time points listed above"
}
json_to_gcs("employees/successful_run_log/log.json", [successful_run],
            bucket)

json_to_gcs(f"{args['out_loc']}", all_records, bucket)
