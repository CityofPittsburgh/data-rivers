import os
import argparse
import pendulum
from datetime import datetime
import requests
from google.cloud import storage

from gcs_utils import json_to_gcs, find_last_successful_run

DEFAULT_RUN_START = "2021-06-04"

storage_client = storage.Client()
bucket = f"{os.environ['GCS_PREFIX']}_cherwell"

parser = argparse.ArgumentParser()
parser.add_argument('--output_arg', dest='out_loc', required=True,
                    help='fully specified location to upload the ndjson file of incident data')
args = vars(parser.parse_args())

# obtain an OAuth 2.0 access token by posting to the Cherwell API before making the data request
auth_url = f"{os.environ['CHERWELL_BASE_URL']}/CherwellAPI/token"
auth_body = f"grant_type=password&client_id={os.environ['CHERWELL_API_KEY']}&username={os.environ['CHERWELL_USER']}&" \
            f"password={os.environ['CHERWELL_PW']}"

oauth = requests.post(auth_url, data=auth_body)
token = oauth.json()['access_token']

# find the last successful DAG run (needs to be specified in UTC YYYY-MM-DD HH:MM:SS)
# if there was no previous good run, default to the date the oldest Cherwell ticket was submitted (3/1/2022).
# this is used to initialize the payload below
run_start_win, first_run = find_last_successful_run(bucket, "surveys/successful_run_log/log.json", DEFAULT_RUN_START)

url = f"{os.environ['CHERWELL_BASE_URL']}/CherwellAPI/api/V1/getsearchresults"
payload = {
    "association": os.environ['CHERWELL_ASSOC'],
    "busObId": "94479e5ee60e4533e7fd934ac9a44cf2c0a05841f9",  # "Customer Satisfaction Survey"
    "fields": [
        "94479e609184166c8e1b9e4eb08630d1888eab0968",  # "SurveyID"
        "9447a3d0b679ef4bc09ea14b6097050852361a0c3c",  # "IncidentID"
        "94479e5f70aad44492b12e4d6f99dc35102e836b03",  # "CreatedDateTime"
        "94479e60efe1e2154487834d3886a350d76e0486e5",  # "SurveySubmittedBy"
        "94479e6129182d9dce92ea434f92847a786f84b699",  # "SurveySubmittedOn"
        "94479e6056cbc43fb4cf5e4817839ba38632d1f3ad",  # "SurveyComplete"
        "94479e5fa9d327bbb9e2924520a5846ed6c4ac9e28",  # "Q1"
        "94479e5fbbc73747afd45b4fc9bd965a7475a08c97",  # "Q2"
        "94479e5fcd9f330a236cd2416fb87f593c73831d1b",  # "Q3"
        "94479e5fe045e00904f72d47028300f2fdd658f64e",  # "Q4"
        "94479e5ff2f3cfdb64340d4949bfc3e5670c998011",  # "Q5"
        "94479e60052b52458abc03434389a506e1499b08ba",  # "Q6"
        "94479e6017f4b24a7f01794edaae3afd98d6f46ab2",  # "Q7"
        "94479e602aaa1cfb9c48c44506b81ecc1032f2c3db",  # "Q8"
        "94479e60ba299be1d6aa654642b08e95bd68e6c92f",  # "SurveyScore"
        "94479e62e56e4422d701f84a17a7a06c7c235659ad",  # "AvgSurveyScore"
        "94479e616884f917c55c2647a094513baf845b895e",  # "OwnedByTechnician"
        "94479e5f700ad0e420781d47e2a263cbdf66d39efa",  # "LastModDateTime"
        "94479e5f703548b8cd86004ecb99c58155761bc4d0"   # "LastModBy"
    ],
    "sorting": [
        {
            "fieldId": "94479e5f70aad44492b12e4d6f99dc35102e836b03",  # "CreatedDateTime"
            "sortDirection": 1  # Sort Ascending
        }
    ],
    "pageSize": 200
}

# initialize page number count, will increase as more pages of records (200 at a time)
page_num = 1
all_records = []
# make use of session to retain authorization header while making multiple requests
s = requests.session()
more = True
while more is True:
    # add new pageNumber parameter to request payload with every iteration
    payload['pageNumber'] = page_num
    # API call to get data
    response = s.post(url, headers={'Content-Type': 'application/json', 'Authorization': f'Bearer {token}'},
                      json=payload)
    # search filter accepts date comparison in YYYY-mm-dd HH:MM format (military time, local time zone)
    # tickets come through in EST time, not UTC
    curr_run = datetime.now(tz=pendulum.timezone('EST')).strftime("%Y-%m-%d %H:%M")
    print(f"Response at {str(curr_run)}: {str(response.status_code)}")

    surveys = response.json()['businessObjects']
    # continue looping through records until the API indicates that there are no more left
    if response.json()['hasMoreRecords']:
        page_num += 1
    else:
        more = False
    # append list of API results to growing all_records list (should not need more than initial API call after 1st run)
    all_records += surveys

# write the successful run information (used by each successive run to find the backfill start date)
successful_run = {
    "requests_retrieved": len(all_records),
    "since": run_start_win,
    "current_run": curr_run,
    "note": "Data retrieved between the time points listed above"
}
json_to_gcs("surveys/successful_run_log/log.json", [successful_run],
            bucket)

json_to_gcs(f"{args['out_loc']}", all_records, bucket)
