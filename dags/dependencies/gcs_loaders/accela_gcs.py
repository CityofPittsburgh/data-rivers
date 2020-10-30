import os
import argparse
import requests
import logging

from gcs_utils import json_to_gcs, filter_fields, execution_date_to_prev_quarter


parser = argparse.ArgumentParser()
parser.add_argument('-e', '--execution_date', dest='execution_date',
                    required=True, help='DAG execution date (YYYY-MM-DD)')
parser.add_argument('-p', '--prev_execution_date', dest='prev_execution_date',
                    required=True, help='Previous DAG execution date (YYYY-MM-DD)')
args = vars(parser.parse_args())

BASE_URL = 'https://apis.accela.com/v4/records'
ACCESS_SCOPE = 'search_records get_records get_record_asis get_record_addresses get_record_professionals ' \
               'get_record_contacts get_record get_ref_modules get_record_owners get_record_inspections get_assets ' \
               'get_inspections get_record_related'
EXPAND_FIELDS = 'addresses,parcels,professionals,contacts,owners,customForms,customTables,assets'

oauth_body = {
    'grant_type': 'password',
    'client_id': os.environ['ACCELA_CLIENT_ID'],
    'client_secret': os.environ['ACCELA_CLIENT_SECRET'],
    'username': os.environ['ACCELA_USER'],
    'password': os.environ['ACCELA_PW'],
    'scope': ACCESS_SCOPE,
    'agency_name': 'Pittsburgh_Pa',
    'environment': 'PROD'
}

bucket = '{}_accela'.format(os.environ['GCS_PREFIX'])


def get_token():
    """
    Get Accela api token via oauth flow
    :return: api token (expires in 24 hours)
    """
    try:
        res = requests.post(
            'https://auth.accela.com/oauth2/token',
            data=oauth_body,
            headers={'Content-Type': 'application/x-www-form-urlencoded'}
        )
        return res.json()['access_token']
    except requests.exceptions.RequestException:
        logging.info('Request for OAuth token failed')


def get_all_record_ids(api_token):
    """
    iterate through pages of records and store their ids,
    bumping offset by 1000 each time until res.json()['page']['hasmore'] = False

    :param api_token: string (result of get_token())
    :return: list of record_ids
    """
    record_ids = []
    offset = 0
    hasmore = True
    while hasmore:
        try:
            res = requests.get(
                BASE_URL,
                headers={'Authorization': api_token},
                params={'limit': 1000,
                        'offset': offset,
                        'module': 'PublicWorks',
                        'updateDateFrom': args['prev_execution_date']
                })

            for record in res.json()['result']:
                record_ids.append(record['value'])

            offset += 1000
            hasmore = res.json()['page']['hasmore']

        except requests.exceptions.RequestException:
            hasmore = False
    return record_ids


def enrich_record(record_id, api_token):
    """

    :param record_id: (str) id from list of record_ids returned by get_all_record_ids
    :param api_token: (str) api token returned by get_token
    :return: (dict) JSON object with permit data
    """
    print(record_id)
    res = requests.get(F"{BASE_URL}/{record_id}",
                       headers={'Authorization': api_token},
                       params={'expand': EXPAND_FIELDS})
    count = 0
    try:
        if res.status_code == 200:
            return res.json()['result'][0]
        elif res.status_code == 401:
            api_token = get_token()
            res = requests.get(F"{BASE_URL}/{record_id}",
                               headers={'Authorization': api_token},
                               params={'expand': EXPAND_FIELDS})
            return res.json()['result'][0]
    except requests.exceptions.SSLError:
        import pdb; pdb.set_trace()
    except requests.exceptions.RequestException:
        pass


api_token = get_token()
record_ids = get_all_record_ids(api_token)
enriched_records = []

for record_id in record_ids:
    enriched_records.append(enrich_record(record_id, api_token))

json_to_gcs('permits/{}/{}/{}_permits.json'.format(args['execution_date'].split('-')[0],
                                                   args['execution_date'].split('-')[1], args['execution_date']),
            enriched_records, bucket)
