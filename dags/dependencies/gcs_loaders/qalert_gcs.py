import os
import argparse

import requests

from gcs_utils import json_to_gcs, time_to_seconds, replace_pii

# set initial values for loading operation
parser = argparse.ArgumentParser()
parser.add_argument('-s', '--since', dest='since', required=True,
                    help='Start param for API pull '
                         '(last successful DAG run as YYYY-MM-DD)')
parser.add_argument('-e', '--execution_date', dest='execution_date',
                    required=True, help='DAG execution date (YYYY-MM-DD)')
args = vars(parser.parse_args())
bucket = f"{os.environ['GCS_PREFIX']}_qalert"

# qscend API requires a value (any value) for the user-agent field
headers = {'User-Agent': 'City of Pittsburgh ETL'}
payload = {'key': os.environ['QALERT_KEY'], 'since': time_to_seconds(args['since'])}
response = requests.get('https://pittsburghpa.qscend.com/qalert/api/v1/requests/changes', params=payload,
                        headers=headers)

# convert the utf-8 encoded string to json
full_requests = response.json()['request']

# Comments must be scrubbed for PII from 311 requests.
# Comments do not follow strict formatting so this is an imperfect approximation.
# Steps: extract fields, detect person names that are followed by hotwords for exclusion (e.g. park or street),
# place an underscore between the detected words to prevent accidental redaction, redact PII the user information in
# activities needs to be scrubbed, but does not need to be screened for hotwords


pre_clean = {"req_comments": []}
for row in full_requests:
    pre_clean["req_comments"].append(row.get("comments", ""))

# convert list to string that is seperated with a delimiter sequence (allows full list to be processed as one string,
# as opposed to mapping it to each element which is 100x slower)
delim_seq = os.environ["DELIM"]
pre_clean["req_comments"] = delim_seq.join(pre_clean["req_comments"])


# scrub pii
scrubbed_req_comments = replace_pii(pre_clean["req_comments"], retain_location  = True)

# convert delim-seperated string back to list of strings
split_req_comments = scrubbed_req_comments.split(delim_seq)

# overwrite the original fields with scrubbed data
for i in range(len(full_requests)):
    full_requests[i]["comments"] = split_req_comments[i].strip()

# load to gcs
target_direc = "requests"
year = args['execution_date'].split('-')[0]
month = args['execution_date'].split('-')[1]
full_date = args['execution_date']
target_path = f"/{target_direc}/{year}/{month}/{full_date}_requests.json"
json_to_gcs(target_path, full_requests, bucket)
