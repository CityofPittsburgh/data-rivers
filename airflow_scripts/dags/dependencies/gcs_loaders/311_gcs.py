import requests
import logging
import os

<<<<<<< HEAD
from gcs_utils import YESTERDAY, now, storage_client, json_to_gcs
=======
from gcs_utils import YESTERDAY, now, storage_client, json_to_gcs, scrub_pii
>>>>>>> 74a14db6899e04e390e0954ad7a708867852e1c2


bucket = '{}_311'.format(os.environ['GCS_PREFIX'])
payload = {'key': os.environ['QALERT_KEY'], 'since': YESTERDAY.strftime('%s')}
# qscend requires a value (any value) for the user-agent field  ¯\_(ツ)_/¯
headers = {'User-Agent': 'City of Pittsburgh ETL'}

<<<<<<< HEAD
request_keys = ['id',
=======
REQUEST_KEYS = ['id',
>>>>>>> 74a14db6899e04e390e0954ad7a708867852e1c2
                'master',
                'addDate',
                'addDateUnix',
                'lastAction',
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
                'district',
                'typeId',
                'typeName',
                'priorityValue',
                'latitude',
                'longitude',
                'origin',
                'priorityToDisplay',
                'resumeDate']

<<<<<<< HEAD
activity_keys = ['actDate',
=======
ACTIVITY_KEYS = ['actDate',
>>>>>>> 74a14db6899e04e390e0954ad7a708867852e1c2
                 'actDateUnix',
                 'code',
                 'codeDesc',
                 'displayDate',
                 'id',
                 'notify',
                 'requestId',
                 'routeId',
                 'reasonId']

response = requests.get('https://pittsburghpa.qscend.com/qalert/api/v1/requests/changes', params=payload, headers=headers)

<<<<<<< HEAD
cleaned_requests = []
cleaned_activities = []

if response.status_code == 200:
    for request in response.json()['request']:
        cleaned_request = {k: request[k] for k in request_keys}
        cleaned_requests.append(cleaned_request)

    for activity in response.json()['activity']:
        cleaned_activity = {k: activity[k] for k in activity_keys}
        cleaned_activities.append(cleaned_activity)


json_to_gcs('{}/{}/{}_requests.json'.format(now.strftime('%Y'),
                                            now.strftime('%m').lower(),
                                            now.strftime("%Y-%m-%d")),
            cleaned_requests, bucket)

json_to_gcs('{}/{}/{}_activities.json'.format(now.strftime('%Y'),
                                              now.strftime('%m').lower(),
                                              now.strftime("%Y-%m-%d")),
            cleaned_activities, bucket)
=======
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
>>>>>>> 74a14db6899e04e390e0954ad7a708867852e1c2
