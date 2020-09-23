import os
import argparse
import requests

from gcs_utils import storage_client, json_to_gcs, time_to_seconds, filter_fields


parser = argparse.ArgumentParser()
parser.add_argument('-s', '--since', dest='since', required=True,
                    help='Start param for API pull '
                         '(last successful DAG run as YYYY-MM-DD)')
parser.add_argument('-e', '--execution_date', dest='execution_date',
                    required=True, help='DAG execution date (YYYY-MM-DD)')
args = vars(parser.parse_args())

bucket = '{}_qalert'.format(os.environ['GCS_PREFIX'])
# qscend API requires a value (any value) for the user-agent field
headers = {'User-Agent': 'City of Pittsburgh ETL'}
payload = {'key': os.environ['QALERT_KEY'], 'since': time_to_seconds(args['since'])}

REQUEST_KEYS = ['id',
                'master',
                'addDateUnix',
                'lastActionUnix',
                'dept',
                'status',
                'typeId',
                'typeName',
                'priorityValue',
                'latitude',
                'longitude',
                'origin']

ACTIVITY_KEYS = ['id',
                 'requestId',
                 'actDateUnix',
                 'code',
                 'codeDesc']

response = requests.get('https://pittsburghpa.qscend.com/qalert/api/v1/requests/changes', params=payload,
                        headers=headers)


# filter responses to take out unnecessary keys, preserving only those we've defined in request/activity_keys

trimmed_requests = filter_fields(response.json()['request'], REQUEST_KEYS)

trimmed_activities = filter_fields(response.json()['activity'], ACTIVITY_KEYS)

json_to_gcs('requests/{}/{}/{}_requests.json'.format(args['execution_date'].split('-')[0],
                                                     args['execution_date'].split('-')[1], args['execution_date']),
            trimmed_requests, bucket)

json_to_gcs('activities/{}/{}/{}_activities.json'.format(args['execution_date'].split('-')[0],
                                                         args['execution_date'].split('-')[1], args['execution_date']),
            trimmed_activities, bucket)
