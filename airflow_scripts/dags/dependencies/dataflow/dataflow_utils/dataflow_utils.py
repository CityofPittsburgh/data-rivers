from __future__ import absolute_import

import os
import scourgify
from scourgify import normalize_address_record, exceptions

from avro import schema
from google.cloud import bigquery, storage


# GOOGLE_APPLICATION_CREDENTIALS = os.environ['GOOGLE_APPLICATION_CREDENTIALS']

bq_client = bigquery.Client()
storage_client = storage.Client()


# monkey patch for avro schema hashing bug: https://issues.apache.org/jira/browse/AVRO-1737
def hash_func(self):
    return hash(str(self))

schema.RecordSchema.__hash__ = hash_func


def download_schema(bucket_name, source_blob_name, destination_file_name):
    """Downloads avro schema from Cloud Storage"""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(source_blob_name)

    blob.download_to_filename(destination_file_name)


def clean_csv_string(string):
    return string.strip('"')


def clean_csv_int(integer):
    return int(integer.strip('"'))


def normalize_address(address):
    normalized_dict = normalize_address_record(address)
    try:
        for k, v in normalized_dict.items():
            if v:
                normalized_string += (str(v) + ' ')
        normalized_string = normalized_string.strip()
        return normalized_string
    except scourgify.exceptions.UnParseableAddressError:
        return ''
