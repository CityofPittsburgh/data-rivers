import os
import argparse
import requests
import logging

from gcs_utils import storage_client, json_to_gcs, time_to_seconds, filter_fields

parser = argparse.ArgumentParser()
parser.add_argument('-s', '--since', dest='since', required=True,
                    help='Start param for API pull '
                         '(last successful DAG run as YYYY-MM-DD)')
parser.add_argument('-e', '--execution_date', dest='execution_date',
                    required=True, help='DAG execution date (YYYY-MM-DD)')
args = vars(parser.parse_args())

bucket = '{}_accela'.format(os.environ['GCS_PREFIX'])

BASE_URL = 'https://apis.accela.com/v4/records'
ACCESS_SCOPE = 'search_records get_records get_record_asis get_record_addresses get_record_professionals ' \
               'get_record_contacts get_record get_ref_modules get_record_owners get_record_inspections get_assets ' \
               'get_inspections get_record_related'
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


def get_token():
    try:
        res = requests.post(
            'https://auth.accela.com/oauth2/token',
            data=oauth_body,
            headers={'Content-Type': 'application/x-www-form-urlencoded'}
        )
        return res.json()['access_token']
    except requests.exceptions.RequestException:
        logging.info('Request for OAuth token failed')


