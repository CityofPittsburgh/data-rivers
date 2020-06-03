from __future__ import absolute_import

import re
import json
from datetime import datetime

from scourgify import normalize_address_record, exceptions
from avro import schema
from google.cloud import bigquery, storage

dt = datetime.now()
bq_client = bigquery.Client()
storage_client = storage.Client()

DEFAULT_DATAFLOW_ARGS = [
    '--project=data-rivers',
    '--subnetwork=https://www.googleapis.com/compute/v1/projects/data-rivers/regions/us-east1/subnetworks/default',
    '--region=us-east1',
    '--service_account_email=data-rivers@data-rivers.iam.gserviceaccount.com',
    '--save_main_session',
]


def generate_args(job_name, bucket, runner):
    arguments = DEFAULT_DATAFLOW_ARGS
    arguments.append('--job_name={}'.format(job_name))
    arguments.append('--runner={}'.format(runner))
    arguments.append('--staging_location=gs://{}/beam_output/staging'.format(bucket))
    arguments.append('--temp_location=gs://{}/beam_output/temp'.format(bucket))
    return arguments


# monkey patch for avro schema hashing bug: https://issues.apache.org/jira/browse/AVRO-1737
def hash_func(self):
    return hash(str(self))

schema.RecordSchema.__hash__ = hash_func


def download_schema(bucket_name, source_blob_name, destination_file_name):
    """Downloads avro schema from Cloud Storage"""
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(source_blob_name)

    blob.download_to_filename(destination_file_name)


def get_schema(schema):
    """Read avsc from cloud storage and return json object stored in memory"""
    bucket = storage_client.get_bucket('pghpa_avro_schemas')
    blob = bucket.get_blob('{}.avsc'.format(schema))
    schema_string = blob.download_as_string()
    return json.loads(schema_string)


def clean_csv_string(string):
    try:
        return string.strip('"').strip()
    except ValueError:
        return None


def clean_csv_int(integer):
    try:
        return int(integer.strip('"').strip())
    except ValueError:
        return None


def clean_csv_float(num):
    try:
        return float(num)
    except ValueError:
        return None

def clean_csv_boolean(boolean):
    try:
        if str(boolean).lower() == 'true':
            return True
        elif str(boolean).lower() == 'false':
            return False
        else: 
            return None
    except ValueError:
        return None


def normalize_address(address):
    text2number = {"ZERO": "0", "ONE": "1", "TWO": "2", "THREE": "3", "FOUR": "4", "FIVE": "5", "SIX": "6",
                   "SEVEN": "7",
                   "EIGHT": "8", "NINE": "9", "TEN": "10", "FIRST": "1ST", "SECOND": "2ND", "THIRD": "3RD",
                   "FOURTH": "4TH",
                   "FIFTH": "5TH", "SIXTH": "6TH", "SEVENTH": "7TH", "EIGHTH": "8TH", "NINTH": "9TH", "TENTH": "10TH"}
    try:
        normalized_string = ""
        pattern = re.compile(r'\b(' + '|'.join(text2number.keys()) + r')\b')
        address_num = pattern.sub(lambda x: text2number[x.group()], address)
        normalized_dict = normalize_address_record(address_num)
        for k, v in normalized_dict.items():
            if v:
                normalized_string += (str(v) + ' ')
        normalized_string = normalized_string.strip()
        return normalized_string
    except exceptions.UnParseableAddressError:  # use original address if unparseable
        return address


class JsonCoder(object):
    """A JSON coder interpreting each line as a JSON string."""

    def encode(self, x):
        return json.dumps(x)

    def decode(self, x):
        return json.loads(x)
