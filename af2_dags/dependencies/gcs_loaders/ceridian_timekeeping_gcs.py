import os
import argparse
import pendulum
import requests
from datetime import datetime, timedelta
from requests.auth import HTTPBasicAuth
from google.cloud import storage

from gcs_utils import json_to_gcs, find_last_successful_run


storage_client = storage.Client()
bucket = f"{os.environ['GCS_PREFIX']}_ceridian"

parser = argparse.ArgumentParser()
parser.add_argument('--output_arg', dest='out_loc', required=True,
                    help='fully specified location to upload the combined ndjson file')
args = vars(parser.parse_args())


# adapted from https://stackoverflow.com/a/43088
def last_day_of_month(datestring):
    input_date = datetime.strptime(datestring, "%m/%d/%Y %H:%M:%S %p")
    if input_date.month == 12:
        return input_date.replace(day=31)
    date_val = input_date.replace(month=input_date.month+1, day=1) - timedelta(days=1)
    return str(date_val.date().strftime("%m/%d/%Y")) + " 11:59:59 PM"


def set_url(first_of_month, last_of_month):
    return f"https://www.dayforcehcm.com/Api/{os.environ['CERIDIAN_ORG_ID']}/V1/Reports/TIMESHEETREPORTAPI" \
                f"?5b93e411-2928-463d-b048-03460269d416={first_of_month}&" \
                f"bb93e411-2928-463d-b048-03460269d416={last_of_month}"


# adapted from https://pynative.com/python-get-last-day-of-month/#h-get-last-day-of-a-previous-month
input_dt = datetime.today()
curr_month_first = input_dt.replace(day=1)
# store the initial first day of the previous month in a variable so the backfill knows when to stop
curr_month_first_str = str(curr_month_first.date().strftime("%m/%d/%Y %H:%M:%S %p"))
month_last = curr_month_first - timedelta(days=1)
month_first = month_last.replace(day=1)

run_start_win, first_run = find_last_successful_run(bucket, "timekeeping/successful_run_log/log.json",
                                                    "01/01/2023 00:00:00 AM")

# if this is the first time the DAG has ran, start pulling data from the beginning of the year
# otherwise, only get the previous month's data
if first_run:
    month_first = run_start_win
else:
    month_first = str(month_first.date().strftime("%m/%d/%Y %H:%M:%S %p"))

# The Ceridian API can only return up to 100K records per request, and each month of data contains about
# 120K rows. This code splits the month in half so all the data can be retrrieved
month_last = (datetime.strptime(month_first, "%m/%d/%Y %H:%M:%S %p")).replace(day=15)
month_last = str(month_last.strftime("%m/%d/%Y")) + " 11:59:59 PM"

# (DAG should never pull data from the current month)
print(f'Initial request date range: {month_first}-{month_last}')

auth = HTTPBasicAuth(os.environ['CERIDIAN_USER'], os.environ['CERIDIAN_PW'])

# set filters on API request - data should encompass the entire previous month
url = set_url(month_first, month_last)

# Make use of session to retain authorization header when a URL redirect happens
s = requests.session()
more = True
all_records = []

while more is True:
    # API call to get data
    response = s.get(url, auth=auth)
    # Initial API should fail due to URL change, response gives us new URL
    if response.status_code == 401:
        redir_url = response.url
        # Retry API request with same parameters but new URL
        response = s.get(redir_url, auth=auth)
    curr_run = datetime.now(tz=pendulum.timezone('UTC')).strftime("%Y-%m-%d %H:%M:%S")
    print(f"Response for time range {month_first} through {month_last}: {str(response.status_code)}")

    time_data = response.json()['Data']['Rows']
    # continue looping through records until the API has a MoreFlag value of 0
    if response.json()['Paging']['Next']:
        url = response.json()['Paging']['Next']
    # just because there is no MoreFlag value does not necessarily mean there is no more data
    # continue looping by moving time window up ~15 days until we catch up to the current month
    elif month_first != curr_month_first_str:
        month_first = datetime.strptime(month_last, "%m/%d/%Y %H:%M:%S %p").date() + timedelta(days=1)
        if '/15/' in month_last:
            month_last = last_day_of_month(month_last)
        else:
            month_last = month_first.replace(day=15)
            month_last = str(month_last.strftime("%m/%d/%Y")) + " 11:59:59 PM"
        month_first = str(month_first.strftime("%m/%d/%Y %H:%M:%S %p"))
        if month_first != curr_month_first_str:
            url = set_url(month_first, month_last)
        else:
            more = False
    else:
        more = False

    # append list of API results to growing all_records list (should require several loops)
    all_records += time_data

print(f"Total extract length: {len(all_records)}")
json_to_gcs(f"{args['out_loc']}", all_records, bucket)

successful_run = [{"since": month_first,
                   "current_run": month_last,
                   "note": "Timekeeping data retrieved between the time points listed above"}]
json_to_gcs("timekeeping/successful_run_log/log.json", successful_run, bucket)
