import os
import ndjson
import logging

from datetime import datetime, timedelta
from google.cloud import storage

YESTERDAY = datetime.combine(datetime.today() - timedelta(1), datetime.min.time())
WEEK_AGO = datetime.combine(datetime.today() - timedelta(7), datetime.min.time())
now = datetime.now()

GOOGLE_APPLICATION_CREDENTIALS = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
storage_client = storage.Client()


def json_to_gcs(path, json_response, bucket_name):
    blob = storage.Blob(
        name=path,
        bucket=storage_client.get_bucket(bucket_name),
    )
    blob.upload_from_string(
        # dataflow needs newline-delimited json, so use ndjson
        data=ndjson.dumps(json_response),
        content_type='application/json',
        client=storage.Client(),
    )
    logging.info(
        'Successfully uploaded blob %r to bucket %r.', path, bucket_name)
