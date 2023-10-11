import os
import argparse
import pendulum
import requests
import time
from datetime import datetime, timedelta
from requests.auth import HTTPBasicAuth
from google.cloud import storage

from gcs_utils import json_to_gcs, get_last_day_of_month


storage_client = storage.Client()
bucket = f"{os.environ['GCS_PREFIX']}_ceridian"

parser = argparse.ArgumentParser()
parser.add_argument('--output_arg', dest='out_loc', required=True,
                    help='fully specified location to upload the combined ndjson file')
args = vars(parser.parse_args())


def set_url(first_of_month, last_of_month):
    return f"https://www.dayforcehcm.com/Api/{os.environ['CERIDIAN_ORG_ID']}/V1/Reports/TIMESHEETREPORTAPI" \
                f"?5b93e411-2928-463d-b048-03460269d416={first_of_month}&" \
                f"bb93e411-2928-463d-b048-03460269d416={last_of_month}"


input_year = args['out_loc'].split('/')[1]
end_year = str(int(input_year)+1)
month_first = f"01/01/{input_year} 00:00:00 AM"
month_last = f"01/15/{input_year} 11:59:59 PM"

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
    elif response.status_code == 429:
        time.sleep(300)
        response = s.get(url, auth=auth)
    curr_run = datetime.now(tz=pendulum.timezone('UTC')).strftime("%Y-%m-%d %H:%M:%S")
    print(f"Response for time range {month_first} through {month_last}: {str(response.status_code)}")
    try:
        time_data = response.json()['Data']['Rows']
        # continue looping through records until the API has a MoreFlag value of 0
        if response.json()['Paging']['Next']:
            url = response.json()['Paging']['Next']
        # just because there is no MoreFlag value does not necessarily mean there is no more data
        # continue looping by moving time window up ~15 days until we catch up to the current month
        elif end_year not in month_first:
            try:
                month_first = datetime.strptime(month_last, "%m/%d/%Y %H:%M:%S %p").date() + timedelta(days=1)
            except TypeError:
                month_first = month_last.date() + timedelta(days=1)
                month_last = str(month_last.strftime("%m/%d/%Y %H:%M:%S %p"))
            if '/15/' in month_last:
                month_last = get_last_day_of_month(month_last, "%m/%d/%Y %H:%M:%S %p", "%m/%d/%Y", " 11:59:59 PM")
            else:
                month_last = month_first.replace(day=15)
                month_last = str(month_last.strftime("%m/%d/%Y")) + " 11:59:59 PM"
            month_first = str(month_first.strftime("%m/%d/%Y %H:%M:%S %p"))
            if end_year not in month_first:
                url = set_url(month_first, month_last)
            else:
                more = False
        else:
            more = False

        # append list of API results to growing all_records list (should require several loops)
        all_records += time_data
    except KeyError:
        print('429 response after 5 minute pause, trying again')

print(f"Total extract length: {len(all_records)}")
json_to_gcs(f"{args['out_loc']}", all_records, bucket)
