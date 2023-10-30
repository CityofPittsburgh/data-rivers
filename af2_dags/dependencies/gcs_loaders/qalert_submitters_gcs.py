import os
import argparse
import requests
import time

from json.decoder import JSONDecodeError
from google.cloud import storage
from gcs_utils import json_to_gcs, get_last_upload


# Initialize the GCS client
client = storage.Client()

parser = argparse.ArgumentParser()
parser.add_argument('--output_arg', dest='out_loc', required=True,
                    help='fully specified location to upload the combined ndjson file')
args = vars(parser.parse_args())

# Retrieve path of most recent Qalert extract - use that as source for requester data
bucket_name = f"{os.environ['GCS_PREFIX']}_qalert"
bucket = client.get_bucket(bucket_name)
log_path = 'requests/successful_run_log/log.json'
known_path = 'requests/backfill/2022-10-14/backfilled_requests.json'
last_upload = get_last_upload(bucket_name, log_path, known_path)


data = []
headers = {'User-Agent': 'City of Pittsburgh ETL'}
payload = {'key': os.environ['QALERT_KEY'], 'stats': True}
for request in last_upload:
    if request['submitter'] != 0:
        payload.update({'id': request['submitter']})
        # API requests need to be made one at a time, which can easily overwhelm the request limit
        # The request attempts loop until a 200 response is obtained
        res_code = '429'
        while res_code == '429':
            response = requests.get('https://pittsburghpa.qscend.com/qalert/api/v1/submitters/get?',
                                    params=payload, headers=headers)
            res_code = str(response.status_code)

            try:
                res_json = response.json()
                res_data = res_json['data']
                res_data.update(res_json['stats'])
                del res_data['id']
                request.update(res_data)
            except JSONDecodeError:
                print('Too many requests made in a short amount of time. Sleeping for 1 minute')
                time.sleep(60)
    else:
        # Give all rows the same keys to give Dataflow a consistent schema to work from
        placeholder = {"firstName": None, "lastName": None, "middleInitial": None, "address": None, "address2": None,
                       "city": None, "state": None, "zip": None, "email": None, "phone": None, "phoneExt": None,
                       "altPhone": None, "altPhoneExt": None, "password": None, "verified": None, "banned": None,
                       "twitterId": None, "twitterScreenName": None, "notifyEmail": None, "notifyPhone": None,
                       "notifyAltPhone": None, "notifyMail": None, "notifyPush": None, "notifyPhoneSms": None,
                       "notifyAltPhoneSms": None, "lastRequest": None, "lastRequestUnix": None, "satisfaction": None,
                       "totalClosed": None, "totalRequests": None, "text": None, "lastModified": None,
                       "hasAccount": None}
        request.update(placeholder)
    # Append combined ticket/submitter data to a growing list
    data.append(request)

json_to_gcs(f"{args['out_loc']}_submitters.json", data, bucket)
