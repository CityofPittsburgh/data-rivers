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
soap_url = 'export.attendance.bo'
request = 'getAssignmentProperties'
auth = HTTPBasicAuth(os.environ['INTIME_USER'], os.environ['INTIME_PW'])
headers = {'Content-Type': 'application/xml'}

start = f'<ns2:{request}Response xmlns:ns2="http://v3.{soap_url}.rise.intimesoft.com/">'
end = f'</ns2:{request}Response>'


run_start_win, first_run = find_last_successful_run(bucket, "activity/successful_run_log/log.json", yesterday)
from_time = run_start_win.split(' ')[0]

# API call to get data
response = post_xml(BASE_URL, envelope=generate_xml(soap_url, request, 'POLICE', from_time, today, prefix=prefix),
                    auth=auth, headers=headers,  res_start=start, res_stop=end)
records = response['root']['return']

# verify the API called returned data (if no new records, then type will be NONE)
if records is not None:
    # write the successful run information (used by each successive DAG run to find the backfill date)
    successful_run = [{"requests_retrieved": len(records),
                       "since": run_start_win,
                       "current_run": today,
                       "note": "Data retrieved between the time points listed above"}]
    json_to_gcs("activity/successful_run_log/log.json", successful_run, bucket)

json_to_gcs(f"{args['out_loc']}", records, bucket)
