import os
import argparse
import pendulum
from datetime import datetime
import requests
from requests.auth import HTTPBasicAuth
from google.cloud import storage

from gcs_utils import json_to_gcs

# the first recorded entry date
DEFAULT_RUN_START = "2015-08-19 12:00:00 AM"

API_LIMIT = 1000
offset = 0

storage_client = storage.Client()
bucket = f"{os.environ['GCS_PREFIX']}_cartegraph"

parser = argparse.ArgumentParser()
parser.add_argument('--output_arg', dest='out_loc', required=True,
                    help='fully specified location to upload the combined ndjson file')
args = vars(parser.parse_args())

BASE_URL = "https://cgweb06.cartegraphoms.com/PittsburghPA/api/v1/classes/cgFacilitiesClass"

auth = HTTPBasicAuth(os.environ['CARTEGRAPH_USER'], os.environ['CARTEGRAPH_PW'])
sort = 'EntryDateField:asc'

all_records = []
more = True
while more is True:
    payload = {'sort': sort, 'limit': API_LIMIT, 'offset': offset}
    # API call to get data
    response = requests.get(BASE_URL, auth=auth, params=payload)
    curr_run = datetime.now(tz = pendulum.timezone('US/Eastern')).strftime("%Y-%m-%d %I:%M:%S %p")
    print("Response at " + str(curr_run) + " with offset value " + str(offset) + ": " + str(response.status_code))

    facilities = response.json()['cgFacilitiesClass']
    total = int(response.json()['_metadata']['totalCount'])
    diff = total - int(offset)

    # append list of API results to growing all_records list
    all_records += facilities

    # continue looping through records until we have captured the total count of records
    if diff <= API_LIMIT:
        more = False
    else:
        offset = int(offset) + API_LIMIT


# write the successful run information for documentation purposes
successful_run = {
    "requests_retrieved": len(all_records),
    "run_date": curr_run,
    "current_run": offset,
    "note": "Data extraction left off with the offset value listed above"
}

json_to_gcs(f"{args['out_loc']}", all_records, bucket)

json_to_gcs("facilities/successful_run_log/log.json", [successful_run], bucket)