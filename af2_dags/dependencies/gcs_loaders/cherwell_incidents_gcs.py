import os
import argparse
import pendulum
from datetime import datetime
import requests
from google.cloud import storage

from gcs_utils import json_to_gcs, find_last_successful_run

DEFAULT_RUN_START = "2022-03-01"

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
run_start_win, first_run = find_last_successful_run(bucket, "incidents/successful_run_log/log.json", DEFAULT_RUN_START)

url = f"{os.environ['CHERWELL_BASE_URL']}/CherwellAPI/api/V1/getsearchresults"
payload = {
    "association": os.environ['CHERWELL_ASSOC'],
    "busObId": "6dd53665c0c24cab86870a21cf6434ae",  # "Incident"
    "fields": [
        "6ae282c55e8e4266ae66ffc070c17fa3",  # "IncidentID"
        "c1e86f31eb2c4c5f8e8615a5189e9b19",  # "CreatedDateTime"
        "5eb3234ae1344c64a19819eda437f18d",  # "Status"
        "936725cd10c735d1dd8c5b4cd4969cb0bd833655f4",  # "Service"
        "9e0b434034e94781ab29598150f388aa",  # "Category"
        "1163fda7e6a44f40bb94d2b47cc58f46",  # "Subcategory"
        "252b836fc72c4149915053ca1131d138",  # "Description"
        "83c36313e97b4e6b9028aff3b401b71c",  # "Priority"
        "93543557882ad94503745843c9a380aa0c380935c8",  # "LastModifiedDateTime"
        "11b6961ee55048b9a7240f7e2d3a2f8d",  # "ClosedDateTime"
        "9339fc404e8d5299b7a7c64de79ab81a1c1ff4306c",  # "AssignedTeam"
        "9339fc404e4c93350bf5be446fb13d693b0bb7f219",  # "AssignedTo"
        "9378a8a981b9164941421e4bc6a17872b700662362",  # "AssignedToManager"
        "9365a6098398ff2551e1c14dd398c466d5a201a9c7",  # "IncidentType"
        "9365b1db4ecb560c538b474ad58f51bf1fb6b101a5",  # "SLARespondByDeadline"
        "9365b4209be3fff3623a4a4d6ab76991c2f01ea109",  # "SLAResolveByDeadline"
        "93670bdf8abe2cd1f92b1f490a90c7b7d684222e13",  # "CallSource"
        "93843c51c65c4feef225de469c9389729162bc750e",  # "Stat_IncidentReopened"
        "93843c5730fc45cfb91e404bafaae5ad5978ebdb22",  # "Stat_DateTimeResponded"
        "93b63c949c847642f72c0c43beb901d6806e74901a",  # "Stat_DateTimeResolved"
        "93843c2301c27644faed9e48328310bffaf04250bc",  # "Stat_NumberOfTouches"
        "93843c454f073f3f574df84a7c8c8d7767e08039af",  # "Stat_NumberOfEscalations"
        "940484fd4dad30b6c511da4f8bb0c6d3b614e28806",  # "RequesterDepartment"
        "9450a00ccdb69786a573374a7ba1b43d74e18a80bb",  # "Requester"
        "9450a01dde5721fb08fef04257b82d73d320191342",  # "OnBehalfOf"
        "93734aaff77b19d1fcfd1d4b4aba1b0af895f25788",  # "CustomerDisplayName"
        "94518efdef63fec0cc82874c0ebb786a01f3c51a6f",  # "InitialAssignedTeam"
        "93db0f16a49b1f0232b5ee498ebfee566d48788088",  # "Comments"
        "93408334d3c89b364bf3b14933a74db085d0b47824"   # "CloseDescription"
    ],
    "sorting": [
        {
            "fieldId": "c1e86f31eb2c4c5f8e8615a5189e9b19",  # "CreatedDateTime"
            "sortDirection": 1  # Sort Ascending
        }
    ],
    "filters": [
        {
            "fieldId": "93543557882ad94503745843c9a380aa0c380935c8",  # "LastModifiedDateTime"
            "operator": "gt",  # Is Greater Than
            "value": run_start_win  # last successful run datetime
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

    incidents = response.json()['businessObjects']
    # continue looping through records until the API indicates that there are no more left
    if response.json()['hasMoreRecords']:
        page_num += 1
    else:
        more = False
    # append list of API results to growing all_records list (should not need more than initial API call after 1st run)
    all_records += incidents

# write the successful run information (used by each successive run to find the backfill start date)
successful_run = {
    "requests_retrieved": len(all_records),
    "since": run_start_win,
    "current_run": curr_run,
    "note": "Data retrieved between the time points listed above"
}
json_to_gcs("incidents/successful_run_log/log.json", [successful_run],
            bucket)

json_to_gcs(f"{args['out_loc']}", all_records, bucket)
