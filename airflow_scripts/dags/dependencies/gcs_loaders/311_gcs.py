import requests
import logging
import os

from gcs_utils import YESTERDAY, now, storage_client, json_to_gcs


bucket = '{}_311'.format(os.environ['GCS_PREFIX'])
payload = {'key': os.environ['QALERT_KEY'], 'since': YESTERDAY.strftime('%s')}
# qscend requires a value (any value) for the user-agent field  ¯\_(ツ)_/¯
headers = {'User-Agent': 'City of Pittsburgh ETL'}

request_keys = ['id',
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
                'comments',
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

activity_keys = ['actDate',
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
