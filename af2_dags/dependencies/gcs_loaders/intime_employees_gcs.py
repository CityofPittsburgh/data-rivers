import os
import argparse
import pendulum
from datetime import datetime
from requests.auth import HTTPBasicAuth
from google.cloud import storage

from gcs_utils import json_to_gcs, find_last_successful_run, generate_xml, post_xml

# the date the first employee record was entered
DEFAULT_RUN_START = "2020-06-11"

storage_client = storage.Client()
bucket = f"{os.environ['GCS_PREFIX']}_intime"

parser = argparse.ArgumentParser()
parser.add_argument('--output_arg', dest='out_loc', required=True,
                    help='fully specified location to upload the combined ndjson file')
args = vars(parser.parse_args())

today = datetime.now(tz=pendulum.timezone("utc")).strftime("%Y-%m-%d")

BASE_URL = 'https://intime2.intimesoft.com/ise/employee/v3/EmployeeAccess'
auth = HTTPBasicAuth(os.environ['INTIME_USER'], os.environ['INTIME_PW'])
soap_url = 'v3.employeeaccess'
request = 'getEmployeeDataList'
headers = {'Content-Type': 'application/xml'}
# these variables allow us to parse the record information from the XML text returned by the API
start = '<ns2:getEmployeeDataListResponse xmlns:ns2="http://v3.employeeaccess.rise.intimesoft.com/">'
end = '</ns2:getEmployeeDataListResponse>'

# find the last successful DAG run (needs to be specified in UTC YYYY-MM-DD HH:MM:SS) if there was no previous good
# run default to the date of the first entry of InTime data (allows for complete backfill).
# this is used to initialize the payload below
run_start_win, first_run = find_last_successful_run(bucket, "employees/successful_run_log/log.json", DEFAULT_RUN_START)
from_time = run_start_win.split(' ')[0]

params = [{'tag': 'branchRef', 'content': 'POLICE'},
          {'tag': 'startDate', 'content': from_time},
          {'tag': 'endDate', 'content': today}]

# API call to get data
response = post_xml(BASE_URL, envelope=generate_xml(soap_url, request, params),
                    auth=auth, headers=headers,  res_start=start, res_stop=end)
records = response['root']['return']

# verify the API called returned data (if no new records, then type will be NONE)
if records is not None:
    # write the successful run information (used by each successive DAG run to find the backfill date)
    successful_run = [{"requests_retrieved": len(records),
                       "since": run_start_win,
                       "current_run": today,
                       "destination_file": args["out_loc"],
                       "note": "Data retrieved between the time points listed above"}]
    json_to_gcs("employees/successful_run_log/log.json", successful_run, bucket)

json_to_gcs(f"{args['out_loc']}", records, bucket)
