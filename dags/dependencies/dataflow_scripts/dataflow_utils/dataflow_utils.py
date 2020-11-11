from __future__ import absolute_import

import argparse
import logging
import re
import json
import os
import pytz

import apache_beam as beam
import requests

from abc import ABC
from datetime import datetime
from apache_beam.options.pipeline_options import PipelineOptions
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
        """
        :param type_changes: list of tuples; each tuple consists of the field we want to change and the new data
        type we want for its value
        """
        self.type_changes = type_changes

    def process(self, datum):
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
    def __init__(self, name_changes):
        """:param name_changes: list of tuples consisting of existing field name + name to which it should be changed"""
        self.name_changes = name_changes

    def process(self, datum):
        for name_change in self.name_changes:
            datum[name_change[1]] = datum[name_change[0]]
            del datum[name_change[0]]

        yield datum


class FilterFields(beam.DoFn):
    def __init__(self, relevant_fields, exclude_relevant_fields=True):
        self.relevant_fields = relevant_fields
        self.exclude_relevant_fields = exclude_relevant_fields

    def process(self, datum):
        if datum is not None:
            datum = filter_fields(datum, self.relevant_fields, self.exclude_relevant_fields)
            yield datum
        else:
            logging.info('got NoneType datum')


class GetDateStrings(beam.DoFn, ABC):
    def __init__(self, date_conversions):
        """
        :param date_conversions: list of tuples; each tuple consists of an existing field name + a name for the
        new date-string field.
        """
        self.date_conversions = date_conversions

    def process(self, datum):
        for column in self.date_conversions:
            datum[column[1]] = unix_to_date_string(datum[column[0]])

        yield datum


class GeocodeAddress(beam.DoFn):

    def __init__(self, address_field):
        self.address_field = address_field

    def process(self, datum):
        geocode_address(datum, self.address_field)

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


def unix_to_date_string(unix_date):
    """
    this function converts unix timestamps (integer type) to human readable UTC timestamps (string type)
    :param unix_date: int
    :return: string
    """
    return pytz.timezone('America/New_York').localize(datetime.fromtimestamp(unix_date)).strftime('%Y-%m-%d %H:%M:%S %Z')


def geocode_address(datum, address_field):
    coords = {'lat': None, 'long': None}
    address = datum[address_field]
    if 'pittsburgh' not in address.lower():
        address += ' pittsburgh'
    try:
        res = requests.get(F"http://gisdata.alleghenycounty.us/arcgis/rest/services/Geocoders/Composite/GeocodeServer/"
                           F"findAddressCandidates?Street=&City=&State=&ZIP=&SingleLine="
                           F"{address.replace(',', '').replace('#', '')}&category=&outFields=&maxLocations=&outSR="
                           F"4326&searchExtent=&location=&distance=&magicKey=&f=pjson")
        if len(res.json()['candidates']):
            coords['lat'] = res.json()['candidates'][0]['location']['y']
            coords['long'] = res.json()['candidates'][0]['location']['x']
        else:
            pass
    except requests.exceptions.RequestException as e:
        pass
    try:
        datum['lat'] = coords['lat']
        datum['long'] = coords['long']
    except TypeError:
        datum['lat'] = None
        datum['long'] = None

    return datum


def extract_field(datum, source_field, nested_field, new_field_name):
    """
    In cases where datum contains nested dicts, traverse to nested dict and extract a value for reassignment to a new
    non-nested field

    :param datum: datum in PCollection
    :param source_field: name of field containing desired nested value
    :param nested_field: name of field nested within source_field dict the value of which we want to extract
    and assign its value to new_field_name
    :param new_field_name: name for new field we're creating with the value of nested_field
    :return: datum in PCollection (dict)
    """
    try:
        datum[new_field_name] = datum[source_field][nested_field]
    except KeyError:
        datum[new_field_name] = None

    return datum


def extract_field_from_nested_list(datum, source_field, list_index, nested_field, new_field_name):
    """
    In cases where datum contains values consisting of lists of dicts, isolate a nested dict within a list and extract
    a value for reassignment to a new non-nested field

    :param datum: datum in PCollection (dict)
    :param source_field: name of field containing desired nested value (str)
    :param list_index: index of relevant nested list contained within source_field (int)
    :param nested_field: name of field nested within the desired list of dicts contained within source_field (str)
    :param new_field_name: name for new field we're creating with the value of nested_field (str)
    :return: datum in PCollection (dict)
    """
    try:
        datum[new_field_name] = datum[source_field][list_index][nested_field]
    except (KeyError, IndexError):
        datum[new_field_name] = None

    return datum


def filter_fields(datum, relevant_fields, exclude_relevant_fields=True):
    """
    :param datum: datum in PCollection (dict)
    :param relevant_fields: list of fields to drop or to preserve (dropping all others) (list)
    :param exclude_relevant_fields: preserve or drop relevant fields arg. we add this as an option because in some cases the list
    of fields we want to preserve is much longer than the list of those we want to drop, and vice verse, so having this
    option allows us to make the hard-coded RELEVANT_FIELDS arg in the dataflow script as terse as possible (bool)
    :return:
    """
    fields_for_deletion = []
    if exclude_relevant_fields:
        for k, v in datum.items():
            if k in relevant_fields:
                fields_for_deletion.append(k)
    else:
        for k, v in datum.items():
            if k not in relevant_fields:
                fields_for_deletion.append(k)

    for field in fields_for_deletion:
        datum.pop(field, None)

    return datum


def sort_dict(d):
    """
    This helper sorts a dict by key. It's useful for testing when have a hard-coded expected variable and we want to
    execute a function that returns a dict, but we're not sure how the keys in the dict returned by that function will
    be sorted. By running this sort_dict on both values, we can ensure that comparing them via assertEqual won't
    fail simply because their keys are in different orders.

    :param d: dict
    :return: dict sorted by key
    """
    return dict(sorted(d.items()))
