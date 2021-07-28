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
bucket = '{}_qalert_refactor'.format(os.environ['GCS_PREFIX'])

# qscend API requires a value (any value) for the user-agent field
headers = {'User-Agent': 'City of Pittsburgh ETL'}
payload = {'key': os.environ['QALERT_KEY'], 'since': time_to_seconds(args['since'])}
response = requests.get('https://pittsburghpa.qscend.com/qalert/api/v1/requests/changes', params=payload,
                        headers=headers)

# convert the utf-8 encoded string to json
full_requests = response.json()['request']
full_activities = response.json()['activity']

# Comments must be scrubbed for PII from both 311 requests and activities.
# Comments do not follow strict formatting so this is an imperfect approximation.
# Steps: extract fields, detect person names that are followed by hotwords for exclusion (e.g. park or street),
# place an underscore between the detected words to prevent accidental redaction, redact PII the user information in
# activities needs to be scrubbed, but does not need to be screened for hotwords


pre_clean = {"req_comments": [], "act_comments": [], "act_user": []}
for row in full_requests:
    pre_clean["req_comments"].append(row.get("comments", ""))

for row in full_activities:
    pre_clean["act_comments"].append(row.get("comments", ""))
    pre_clean["act_user"].append(row.get("user", ""))

# convert list to string that is seperated with a delimiter sequence (allows full list to be processed as one string,
# as opposed to mapping it to each element which is 100x slower)
delim_seq = os.environ["DELIM"]
pre_clean["req_comments"] = delim_seq.join(pre_clean["req_comments"])
pre_clean["act_comments"] = delim_seq.join(pre_clean["act_comments"])
pre_clean["act_user"] = delim_seq.join(pre_clean["act_user"])

# # scrub pii
scrubbed_req_comments = replace_pii(pre_clean["req_comments"], retain_location = True)
scrubbed_act_comments = replace_pii(pre_clean["act_comments"], retain_location = True)
scrubbed_act_user = replace_pii(pre_clean["act_user"], retain_location = False)

# convert delim-seperated string back to list of strings
split_req_comments = scrubbed_req_comments.split(delim_seq)
split_act_comments = scrubbed_act_comments.split(delim_seq)
split_act_user = scrubbed_act_user.split(delim_seq)

# overwrite the original fields with scrubbed data
for i in range(len(full_requests)):
    full_requests[i]["comments"] = split_req_comments[i].strip()

for i in range(len(full_activities)):
    full_activities[i]["comments"] = split_act_comments[i].strip()
    full_activities[i]["user"] = split_act_user[i].strip()

# load to gcs
json_to_gcs('requests/{}/{}/{}_requests.json'.format(args['execution_date'].split('-')[0],
                                                     args['execution_date'].split('-')[1],
                                                     args['execution_date']),
            full_requests, bucket)
json_to_gcs('activities/{}/{}/{}_activities.json'.format(args['execution_date'].split('-')[0],
                                                         args['execution_date'].split('-')[1],
                                                         args['execution_date']),
            full_activities, bucket)