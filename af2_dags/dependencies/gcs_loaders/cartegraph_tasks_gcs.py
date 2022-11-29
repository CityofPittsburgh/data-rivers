import os
import argparse
import pendulum
from datetime import datetime, timedelta
import requests
from requests.auth import HTTPBasicAuth
from google.cloud import storage

from gcs_utils import json_to_gcs, find_last_successful_run

# the first recorded entry date
DEFAULT_RUN_START = "2015-08-12 12:00:00 AM"

API_LIMIT = 1000
OFFSET_CAP = 25000

storage_client = storage.Client()
bucket = f"{os.environ['GCS_PREFIX']}_cartegraph"

parser = argparse.ArgumentParser()
parser.add_argument('--output_arg', dest='out_loc', required=True,
                    help='fully specified location to upload the combined ndjson file')
args = vars(parser.parse_args())

# find the last successful DAG run (needs to be specified in UTC YYYY-MM-DD HH:MM:SS)
# if there was no previous good run, default to yesterday's date.
# this is used to initialize the payload below
offset, first_run = find_last_successful_run(bucket, "tasks/successful_run_log/log.json", 0)

BASE_URL = f"https://cgweb06.cartegraphoms.com/PittsburghPA/api/v1/classes/cgTasksClass"

auth = HTTPBasicAuth(os.environ['CARTEGRAPH_USER'], os.environ['CARTEGRAPH_PW'])
sort = 'EntryDateField:asc'
fields = 'Oid,ActivityField,DepartmentField,StatusField,EntryDateField,StartDateActualField,StopDateActualField,' \
         'LaborCostActualField,EquipmentCostActualField,MaterialCostActualField,LaborHoursActualField,' \
         'RequestIssueField,RequestDepartmentField,RequestLocationField,cgAssetIDField,cgAssetTypeField,' \
         'TaskDescriptionField,NotesField,CgShape'

all_records = []
more = True
while more is True:
    payload = {'fields': fields, 'sort': sort, 'limit': API_LIMIT, 'offset': offset}
    # API call to get data
    response = requests.get(BASE_URL, auth=auth, params=payload)
    curr_run = datetime.now(tz = pendulum.timezone('US/Eastern')).strftime("%Y-%m-%d %I:%M:%S %p")
    print("Response at " + str(curr_run) + " with offset value " + str(offset) + ": " + str(response.status_code))

    tasks = response.json()['cgTasksClass']
    total = int(response.json()['_metadata']['totalCount'])
    diff = total - int(offset)

    # append list of API results to growing all_records list
    all_records += tasks

    # continue looping through records until we have captured the total count of records
    if (diff <= API_LIMIT) or (len(all_records) >= OFFSET_CAP):
        more = False
    else:
        offset = int(offset) + API_LIMIT


# write the successful run information (used by each successive run to find the backfill start date)
successful_run = {
    "requests_retrieved": len(all_records),
    "run_date": curr_run,
    "current_run": offset,
    "note": "Data extraction left off with the offset value listed above"
}

json_to_gcs(f"{args['out_loc']}", all_records, bucket)

json_to_gcs("tasks/successful_run_log/log.json", [successful_run], bucket)