from __future__ import absolute_import

import argparse
import re
import json
import os

import apache_beam as beam

from abc import ABC
from datetime import datetime
from apache_beam.options.pipeline_options import PipelineOptions
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


class JsonCoder(object):
    """A JSON coder interpreting each line as a JSON string."""

    def encode(self, x):
        return json.dumps(x)

    def decode(self, x):
        return json.loads(x)


class ColumnsCamelToSnakeCase(beam.DoFn, ABC):
    def process(self, datum):
        cleaned_datum = {camel_to_snake_case(k): v for k, v in datum.items()}
        yield cleaned_datum


class ColumnsToLowerCase(beam.DoFn, ABC):
    def process(self, datum):
        cleaned_datum = {k.lower(): v for k, v in datum.items()}
        yield cleaned_datum


class ChangeDataTypes(beam.DoFn, ABC):
    def __init__(self, type_changes):
        self.type_changes = type_changes

    def process(self, datum):
        """
        :param datum: dict
        :param type_changes: list of tuples of new data type + field to change
        :return: dict
        """
        try:
            for type_change in self.type_changes:
                if type_change[1] == "float":
                    datum[type_change[0]] = float(datum[type_change[0]])
                elif type_change[1] == "int":
                    datum[type_change[0]] = int(datum[type_change[0]])
                elif type_change[1] == "str":
                    datum[type_change[0]] = str(datum[type_change[0]])
                elif type_change[1] == "bool":
                    datum[type_change[0]] = bool(datum[type_change[0]])
        except TypeError:
            pass

        yield datum


class SwapFieldNames(beam.DoFn, ABC):
    """
    :param name_changes: list of tuples consisting of existing field name + name to which it should be changed
    :return dict with updated field names (datum in pcollection)
    """

    def __init__(self, name_changes):
        self.name_changes = name_changes

    def process(self, datum):
        for name_change in self.name_changes:
            datum[name_change[1]] = datum[name_change[0]]
            del datum[name_change[0]]

        yield datum


class GetDateStrings(beam.DoFn, ABC):
    """
    :param date_conversions: list of tuples consisting of existing field name + name for converted string field
    :return dict (datum in pcollection)
    """

    def __init__(self, date_conversions):
        self.date_conversions = date_conversions

    def process(self, datum):
        for conversion in self.date_conversions:
            datum[conversion[1]] = unix_to_date_string(datum[conversion[0]])

        yield datum


class NormalizeAddress(beam.DoFn, ABC):
    """
    intelligently parse/normalize address string according to USPS pub 28 and RESO guidelines

    :param address_key: Beam StaticValueProvider (string)
    :return: string
    """

    def __init__(self, address_key):
        self.address_key = address_key

    def process(self, datum):
        datum['normalized_address'] = normalize_address(datum[self.address_key.get()])

        yield datum


def generate_args(job_name, bucket, argv, schema_name):
    """
    generate arguments for DataFlow jobs (invoked in DataFlow scripts prior to execution)

    :param job_name: name for DataFlow job (string)
    :param bucket: Google Cloud Storage bucket to which avro files will be uploaded (string)
    :param argv: arg parser object (this will always be passed as 'argv=argv' in DataFlow scripts)
    :param schema_name: Name of avro schema file in Google Cloud Storage against which datums will be validated
    :return: known_args (arg parser values), Beam PipelineOptions instance, avro_schemas stored as dict in memory

    Add --runner='DirectRunner' to execute a script locally for rapid development, e.g.
    python qalert_activities_dataflow.py --input gs://pghpa_test_qalert/activities/2020/09/2020-09-23_activities.json
    --avro_output gs://pghpa_test_qalert/activities/avro_output/2020/09/2020-09-23/ --runner DirectRunner

    """
    parser = argparse.ArgumentParser()

    parser.add_argument('--input', dest='input', required=True)
    parser.add_argument('--avro_output', dest='avro_output', required=True)
    parser.add_argument('--runner', '-r', dest='runner', default='DataflowRunner', required=False)

    known_args, pipeline_args = parser.parse_known_args(argv)

    arguments = DEFAULT_DATAFLOW_ARGS
    arguments.append('--job_name={}'.format(job_name))
    arguments.append('--staging_location=gs://{}/beam_output/staging'.format(bucket))
    arguments.append('--temp_location=gs://{}/beam_output/temp'.format(bucket))
    arguments.append('--runner={}'.format(vars(known_args)['runner']))
    arguments.append('--setup_file={}'.format(os.environ['SETUP_PY_DATAFLOW']))
    # ^this doesn't work when added to DEFAULT_DATFLOW_ARGS, for reasons unclear

    pipeline_args.extend(arguments)
    pipeline_options = PipelineOptions(pipeline_args)

    avro_schema = get_schema(schema_name)

    return known_args, pipeline_options, avro_schema


# monkey patch for avro schema hashing bug: https://issues.apache.org/jira/browse/AVRO-1737
def hash_func(self):
    return hash(str(self))


schema.RecordSchema.__hash__ = hash_func


def download_schema(bucket_name, source_blob_name, destination_file_name):
    """Downloads avro schema from Cloud Storage"""
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(source_blob_name)

    blob.download_to_filename(destination_file_name)


def get_schema(schema_name):
    """Read avsc from cloud storage and return json object stored in memory"""
    bucket = storage_client.get_bucket('pghpa_avro_schemas')
    blob = bucket.get_blob('{}.avsc'.format(schema_name))
    schema_string = blob.download_as_string()
    return json.loads(schema_string)


def camel_to_snake_case(val):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', val)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


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


def unix_to_date_string(unix_date):
    """
    return human-readable date from unix timestamp
    :param unix_date: int
    :return: string
    """
    return datetime.fromtimestamp(unix_date).strftime('%Y-%m-%d %H:%M:%S')
