import os
import time
import argparse
import pendulum
from datetime import datetime, timedelta
import xmltodict
import requests
from requests.auth import HTTPBasicAuth
from google.cloud import storage

from gcs_utils import json_to_gcs, find_last_successful_run

# the date the first employee record was entered
DEFAULT_RUN_START = "2020-06-11"

storage_client = storage.Client()
bucket = f"{os.environ['GCS_PREFIX']}_intime"

parser = argparse.ArgumentParser()
parser.add_argument('--output_arg', dest='out_loc', required=True,
                    help='fully specified location to upload the combined ndjson file')
args = vars(parser.parse_args())

today = datetime.now(tz = pendulum.timezone("utc")).strftime("%Y-%m-%d")

BASE_URL = 'https://intime2.intimesoft.com/ise/employee/v3/EmployeeAccess'
auth = HTTPBasicAuth(os.environ['INTIME_USER'], os.environ['INTIME_PW'])


def generate_xml(branch, from_time, to_time):
    """
    :param branch: string to identify the department employee information will be pulled from
    :param from_time: date string in %Y-%m-%d format that identifies the start window for when employee data
    should start being pulled. This date is either the date of the first-ever entry of data into
    the InTime system, or the date of the last successful run of this data pipeline
    :param to_time: date string in %Y-%m-%d format that identifies the end window for when employee data
    should stop being pulled. Should always be the current date
    """
    return F"""
    <S:Envelope xmlns:S="http://schemas.xmlsoap.org/soap/envelope/" xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/" xmlns:v3="http://v3.employeeaccess.rise.intimesoft.com/">
        <S:Body>
            <v3:getEmployeeDataList>
                <branchRef>{branch}</branchRef>
                <startDate>{from_time}</startDate>
                <endDate>{to_time}</endDate>
            </v3:getEmployeeDataList>
        </S:Body>
    </S:Envelope>
    """

headers = {'Content-Type': 'application/xml'}
# these variables allow us to parse the record information from the XML text returned by the API
start = '<ns2:getEmployeeDataListResponse xmlns:ns2="http://v3.employeeaccess.rise.intimesoft.com/">'
end = '</ns2:getEmployeeDataListResponse>'

# find the last successful DAG run (needs to be specified in UTC YYYY-MM-DD HH:MM:SS) if there was no previous good
# run default to the date of the first entry of InTime data (allows for complete backfill).
# this is used to initialize the payload below
run_start_win, first_run = find_last_successful_run(bucket, "successful_run_log/log.json", DEFAULT_RUN_START)
from_time = run_start_win.split(' ')[0]

# continue running the API until data is retrieved (wait 5 min if there is no new data between last_good_run and now (
# curr_run))
data_retrieved = False
while data_retrieved is False:
    # API call to get data
    response = requests.post(BASE_URL, data=generate_xml('POLICE', from_time, today), auth=auth, headers=headers)
    # Print API status code for debugging purposes
    print("API response code: " + str(response.status_code))
    vals = response.text[response.text.find(start) + len(start):response.text.rfind(end)]
    vals = '<root>' + vals + '</root>'
    xml_dict = xmltodict.parse(xml_input=vals, encoding='utf-8')
    records = xml_dict['root']['return']
    # verify the API called returned data (if no new records, then type will be NONE)
    if records is not None:
        data_retrieved = True
        # write the successful run information (used by each successive DAG run to find the backfill date)
        successful_run = [{"requests_retrieved": len(records),
                           "since": run_start_win,
                           "current_run": today,
                           "note": "Data retrieved between the time points listed above"}]
        json_to_gcs("successful_run_log/log.json", successful_run,
                    bucket)

    else:
        print("No new requests. Sleeping with retry scheduled.")
        time.sleep(300)


json_to_gcs(f"{args['out_loc']}", records, bucket)
