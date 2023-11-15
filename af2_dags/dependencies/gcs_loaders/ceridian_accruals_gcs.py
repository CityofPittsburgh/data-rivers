import os
import argparse
import requests
import pendulum

from datetime import datetime
from requests.auth import HTTPBasicAuth
from google.cloud import storage

from gcs_utils import json_to_gcs

storage_client = storage.Client()
bucket = f"{os.environ['GCS_PREFIX']}_ceridian"

parser = argparse.ArgumentParser()
parser.add_argument('--output_arg', dest='out_loc', required=True,
                    help='fully specified location to upload the combined ndjson file')
args = vars(parser.parse_args())

today = datetime.now(tz=pendulum.timezone("utc")).strftime("%Y-%m-%d")

BASE_URL = f"https://www.dayforcehcm.com/Api/{os.environ['CERIDIAN_ORG_ID']}/V1/Reports/ACCRUALSREPORTPOLICE"
auth = HTTPBasicAuth(os.environ['CERIDIAN_USER'], os.environ['CERIDIAN_PW'])

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
    print(f"API Response: {str(response.status_code)}")

    accruals = response.json()['Data']['Rows']
    # continue looping through records until the API has a MoreFlag value of 0
    if response.json()['Paging']['Next']:
        url = response.json()['Paging']['Next']
    else:
        more = False
    # append list of API results to growing all_records list (should not need more than initial API call)
    all_records += accruals

all_records = [{**rec, 'date': datetime.now(tz=pendulum.timezone("utc")).strftime("%Y-%m-%d")} for rec in all_records]
print(len(all_records))
json_to_gcs(f"{args['out_loc']}", all_records, bucket)
