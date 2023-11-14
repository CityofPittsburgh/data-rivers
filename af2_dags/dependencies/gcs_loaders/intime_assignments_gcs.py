import os
import argparse
import pendulum
from datetime import datetime, timedelta
from requests.auth import HTTPBasicAuth
from google.cloud import storage

from gcs_utils import json_to_gcs, find_last_successful_run, generate_xml, post_xml

storage_client = storage.Client()
bucket = f"{os.environ['GCS_PREFIX']}_intime"

parser = argparse.ArgumentParser()
parser.add_argument('--output_arg', dest='out_loc', required=True,
                    help='fully specified location to upload the combined ndjson file')
args = vars(parser.parse_args())

dt = datetime.now(tz=pendulum.timezone("utc"))
today = dt.strftime("%Y-%m-%d")
yesterday = datetime.combine(dt - timedelta(1), datetime.min.time()).strftime("%Y-%m-%d")

BASE_URL = 'https://intime2.intimesoft.com/ise/attendance/v3/ScheduleData'
prefix = 'tns'
soap_url = 'v3.export.attendance.bo'
request = 'getAssignmentProperties'
auth = HTTPBasicAuth(os.environ['INTIME_USER'], os.environ['INTIME_PW'])
headers = {'Content-Type': 'application/xml'}
start = f'<ns2:{request}Response xmlns:ns2="http://v3.{soap_url}.rise.intimesoft.com/">'
end = f'</ns2:{request}Response>'


run_start_win, first_run = find_last_successful_run(bucket, "assignments/successful_run_log/log.json", yesterday)
from_time = run_start_win.split(' ')[0]

params = [{'tag': 'branchReference', 'content': 'POLICE'},
          {'tag': 'startDate', 'content': from_time},
          {'tag': 'endDate', 'content': today}]

# API call to get data
response = post_xml(BASE_URL, envelope=generate_xml(soap_url, request, params, prefix=prefix),
                    auth=auth, headers=headers,  res_start=start, res_stop=end)
records = response['root']['return']

# un-nest data to only extract assignment details
unnested = [rec['assignmentPropertiesData'] if isinstance(rec['assignmentPropertiesData'], dict) else
            rec['assignmentPropertiesData'] for rec in records]
# sometimes one employee will have multiple assignments listed within the current time window
# this code returns each assignment as a separate row of data
unnested = [{**sl, 'sub_assignment': False, 'parent_assignment_id': None}
            for sublist in unnested
            for sl in (sublist if isinstance(sublist, list) else [sublist])]

# verify the API called returned data (if no new records, then type will be NONE)
if unnested is not None:
    sub_activities_list = [
        {'employeeId': item['employeeId'], 'employeeFullName': item['employeeFullName'], **block,
         'sub_assignment': True, 'parent_assignment_id':  item['assignmentId']}
        for item in unnested
        if 'blocks' in item
        for block in (item['blocks'] if isinstance(item['blocks'], list) else [item['blocks']])
    ]
    unnested.extend(sub_activities_list)

    # write the successful run information (used by each successive DAG run to find the backfill date)
    successful_run = [{"requests_retrieved": len(unnested),
                       "since": run_start_win,
                       "current_run": today,
                       "note": "Data retrieved between the time points listed above"}]
    json_to_gcs("assignments/successful_run_log/log.json", successful_run, bucket)

json_to_gcs(f"{args['out_loc']}", unnested, bucket)
