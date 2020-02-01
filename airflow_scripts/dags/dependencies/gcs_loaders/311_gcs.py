import requests
import json
import logging
import ndjson
import os

from gcs_utils import YESTERDAY, dt, storage_client


bucket = '{}_311'.format(os.environ['GCS_PREFIX'])
payload = {'key': os.environ['QALERT_KEY'], 'since': YESTERDAY.strftime('%s')}
# qscend requires a value (any value) for the user-agent field  ¯\_(ツ)_/¯
headers = {'User-Agent': 'City of Pittsburgh ETL'}


def json_to_gcs(self, path, json_response, bucket_name):
    blob = storage_client.Blob(
        name=path,
        bucket=self.get_bucket(bucket_name),
    )
    blob.upload_from_string(
        # dataflow needs newline-delimited json, so use ndjson
        data=ndjson.dumps(json_response),
        content_type='application/json',
        client=self._client,
    )
    logging.info(
        'Successfully uploaded blob %r to bucket %r.', path, bucket_name)


response = requests.get('https://pittsburghpa.qscend.com/qalert/api/v1/requests/changes', params=payload, headers=headers)

json_to_gcs('{}/{}/{}_requests.json'.format(dt.strftime('%Y'),
                                            dt.strftime('%m').lower(),
                                            dt.strftime("%Y-%m-%d")),
            response.json()['request'], bucket)

json_to_gcs('{}/{}/{}_activities.json'.format(dt.strftime('%Y'),
                                              dt.strftime('%m').lower(),
                                              dt.strftime("%Y-%m-%d")),
            response.json()['request'], bucket)

