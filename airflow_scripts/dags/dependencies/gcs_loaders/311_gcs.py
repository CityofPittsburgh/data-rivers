import requests
import logging
import os

from gcs_utils import yesterday, now, storage_client, json_to_gcs


bucket = '{}_311'.format(os.environ['GCS_PREFIX'])
payload = {'key': os.environ['QALERT_KEY'], 'since': yesterday.strftime('%s')}
# qscend requires a value (any value) for the user-agent field  ¯\_(ツ)_/¯
headers = {'User-Agent': 'City of Pittsburgh ETL'}


REQUEST_KEYS = ['id',
                'master',
                'addDateUnix',
                'lastActionUnix',
                'dept',
                'displayDate',
                'displayLastAction',
                'status',
                'streetId',
                'streetName',
                'streetNum',
                'crossStreetId',
                'crossStreetName',
                'typeId',
                'typeName',
                'priorityValue',
                'latitude',
                'longitude',
                'origin',
                'priorityToDisplay',
                'resumeDate']

ACTIVITY_KEYS = ['actDateUnix',
                 'code',
                 'codeDesc',
                 'displayDate',
                 'id',
                 'notify',
                 'requestId',
                 'routeId',
                 'reasonId']

response = requests.get('https://pittsburghpa.qscend.com/qalert/api/v1/requests/changes', params=payload, headers=headers)

trimmed_requests = []
trimmed_activities = []

# filter responses to take out unnecessary keys, preserving only those we've defined in request/activity_keys
if response.status_code == 200:
    for request in response.json()['request']:
        trimmed_request = {k: request[k] for k in REQUEST_KEYS}
        trimmed_requests.append(trimmed_request)

    for activity in response.json()['activity']:
        trimmed_activity = {k: activity[k] for k in ACTIVITY_KEYS}
        trimmed_activities.append(trimmed_activity)

json_to_gcs('requests/{}/{}/{}_requests.json'.format(now.strftime('%Y'),
                                            now.strftime('%m').lower(),
                                            now.strftime("%Y-%m-%d")),
            trimmed_requests, bucket)

json_to_gcs('activities/{}/{}/{}_activities.json'.format(now.strftime('%Y'),
                                              now.strftime('%m').lower(),
                                              now.strftime("%Y-%m-%d")),
            trimmed_activities, bucket)
