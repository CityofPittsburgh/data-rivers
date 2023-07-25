import os
import argparse
import pendulum
import requests
import pandas as pd
from datetime import datetime, timedelta
from requests.auth import HTTPBasicAuth
from google.cloud import storage

from gcs_utils import json_to_gcs, find_last_successful_run

API_LIMIT = 5000

storage_client = storage.Client()
bucket = f"{os.environ['GCS_PREFIX']}_ceridian"

# parser = argparse.ArgumentParser()
# parser.add_argument('--output_arg', dest='out_loc', required=True,
#                     help='fully specified location to upload the combined ndjson file')
# args = vars(parser.parse_args())

# adapted from https://pynative.com/python-get-last-day-of-month/#h-get-last-day-of-a-previous-month
input_dt = datetime.today()
first_curr_month = input_dt.replace(day=1)

prev_month_last = first_curr_month - timedelta(days=1)
prev_month_first = prev_month_last.replace(day=1)

prev_month_last = str(prev_month_last.date().strftime("%m/%d/%Y")) + " 11:59:59 PM"
prev_month_first = str(prev_month_first.date().strftime("%m/%d/%Y %H:%M:%S %p"))
print(f'Previous month date range: {prev_month_first}-{prev_month_last}')

BASE_URL = f"https://www.dayforcehcm.com/Api/{os.environ['CERIDIAN_ORG_ID']}/V1/Reports/TIMESHEETREPORT"
auth = HTTPBasicAuth(os.environ['CERIDIAN_USER'], os.environ['CERIDIAN_PW'])

all_records = []
url = BASE_URL + f"?5b93e411-2928-463d-b048-03460269d416={prev_month_first}&" \
                 f"bb93e411-2928-463d-b048-03460269d416={prev_month_last}"
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
    curr_run = datetime.now(tz=pendulum.timezone('UTC')).strftime("%Y-%m-%d %H:%M:%S")
    print(f"Response at {str(curr_run)}: {str(response.status_code)}")

    time_data = response.json()['Data']['Rows']
    # continue looping through records until the API has a MoreFlag value of 0
    if response.json()['Paging']['Next']:
        url = response.json()['Paging']['Next']
    else:
        more = False
    # append list of API results to growing all_records list (should not need more than initial API call)
    all_records += time_data

# write the successful run information for logging purposes
successful_run = {
    "requests_retrieved": len(all_records),
    "from": prev_month_first,
    "to": prev_month_last,
    "note": "Data retrieved between the time points listed above"
}
json_to_gcs("timekeeping/successful_run_log/log.json", [successful_run], bucket)

# json_to_gcs(f"{args['out_loc']}", all_records, bucket)

df = pd.DataFrame(all_records)
df.head()
