from __future__ import absolute_import

import argparse
import logging
import re
import json
import os
import pytz
import math

import apache_beam as beam
import requests

from abc import ABC
from datetime import datetime
from apache_beam.options.pipeline_options import PipelineOptions
from avro import schema
from google.cloud import bigquery, storage
from dateutil import parser

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
                if type(datum[type_change[0]]) == int or type(datum[type_change[0]]) == float:
                    if math.isnan(datum[type_change[0]]):
                        datum[type_change[0]] = None
                        continue
                try:
                    if type_change[1] == "float":
                        datum[type_change[0]] = float(datum[type_change[0]])
                    elif type_change[1] == "int":
                        datum[type_change[0]] = int(datum[type_change[0]])
                    elif type_change[1] == "str":
                        datum[type_change[0]] = str(datum[type_change[0]])
                    elif type_change[1] == "bool":
                        datum[type_change[0]] = bool(datum[type_change[0]])
                except ValueError:
                    datum[type_change[0]] = None
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


class GetDateStringsFromUnix(beam.DoFn, ABC):
    def __init__(self, date_conversions):
        """
        :param date_conversions: list of tuples; each tuple consists of an existing field name +
         a name for the two new date-string fields.
        """
        self.date_conversions = date_conversions

    def process(self, datum):
        for column in self.date_conversions:
            unix_conv_utc, unix_conv_east = unix_to_date_strings(datum[column[0]])
            datum[column[1]] = unix_conv_utc
            datum[column[2]] = unix_conv_east

        yield datum


class GoogleMapsClassifyAndGeocode(beam.DoFn, ABC):
    def __init__(self, partioned_address, loc_field_names):
        """
        :param partitioned_address: a boolean that idenitifies whether an address is broken into multiple components
        :param loc_field_names: dictionary of 7 field name keys that contain the following information:
        :param address_field: name of field that contains single-line addresses
        :param street_num_field: name of field that contains house numbers
        :param street_name_field: name of field that contains street address names
        :param cross_street_field: name of field that contains intersecting street names
        :param city_field: name of field that contains the city a given street address belongs to
        :param lat_field: name of field that contains the latitude of an address
        :param long_field: name of field that contains the longitude of an address
        """
        self.partioned_address = partioned_address
        if partioned_address:
            self.street_num_field = loc_field_names["street_num_field"]
            self.street_name_field = loc_field_names["street_name_field"]
            self.cross_street_field = loc_field_names["cross_street_field"]
            self.city_field = loc_field_names["city_field"]
        else:
            self.address_field = loc_field_names["address_field"]
        self.lat_field = loc_field_names["lat_field"]
        self.long_field = loc_field_names["long_field"]

    def process(self, datum):
        datum['api_specified_address'] = None
        datum['google_formatted_address'] = None
        datum['address_type'] = None

        # if self.address_field not in datum:
        if self.partioned_address:
            datum = id_underspecified_addresses(datum, self)
        if datum['address_type'] not in ['Missing', 'Coordinates Only']:
            datum = regularize_and_geocode_address(datum, self)

        yield datum


class GeocodeAddress(beam.DoFn):

    def __init__(self, address_field):
        self.address_field = address_field

    def process(self, datum):
        geocode_address(datum, self.address_field)

        yield datum


class StandardizeTimes(beam.DoFn, ABC):
    def __init__(self, time_changes):
        """
        :param time_changes: list of tuples; each tuple consists of an existing field name containing date strings +
        the name of the timezone the given date string belongs to.
        The function takes in date string values and standardizes them to datetimes in UTC, Eastern, and Unix.
        formats. It is powerful enough to handle datetimes in a variety of timezones and string formats.
        The user must provide a timezone name contained within pytz.all_timezones.
        As of June 2021, a list of accepted timezones can be found on https://en.wikipedia.org/wiki/List_of_tz_database_time_zones#List
        Please note that the timezone names are subject to change and the code would have to be updated accordingly. (JF)
        """
        self.time_changes = time_changes

    def process(self, datum):
        for time_change in self.time_changes:
            parse_dt = parser.parse(datum[time_change[0]])
            clean_dt = parse_dt.replace(tzinfo=None)
            try:
                pytz.all_timezones.index(time_change[1])
            except ValueError:
                pass
            else:
                loc_time = pytz.timezone(time_change[1]).localize(clean_dt, is_dst=None)
                utc_conv = loc_time.astimezone(tz=pytz.utc)
                east_conv = loc_time.astimezone(tz=pytz.timezone('America/New_York'))
                unix_conv = utc_conv.timestamp()
                datum.update({'{}_UTC'.format(time_change[0]): str(utc_conv),
                              '{}_EAST'.format(time_change[0]): str(east_conv),
                              '{}_UNIX'.format(time_change[0]): unix_conv})

        yield datum


class ReformatPhoneNumbers(beam.DoFn):
    """
    Method to standardize phone number format according to North American Number Plan.
    Step 1 - Filter out only the digits by cleaning the input string
             Remove commas, punctuations, leading/lagging white spaces, special characters and alphabets
    Step 2 - Separate the country code and area code from the phone number. Default country code is +1
    Step 3 - Format it into +x (xxx) xxx-xxxx     +CountryCode (AreaCode) xxx-xxxx
    """
    def process(self, datum):
        """
        :param datum - Non formatted phone number with/without country code
        """
        digits = "".join(re.findall(r'\d+', datum))
        regex = r'\d{4}$|\d{3}'

        if len(digits) > 10:
            yield "+" + digits[:-10] + " (%s) %s-%s" % tuple(re.findall(regex, digits[-10:]))
        else:
            yield "+1" + " (%s) %s-%s" % tuple(re.findall(regex, digits))


class LatLongReformat(beam.DoFn, ABC):
    def __init__(self, accuracy=200):
        """
        :param accuracy - desired meter accuracy of the lat-long coordinates after rounding the decimals. Default 200m

        var: accuracy_converter - Dictionary of Accuracy versus decimal places whose values represent the number of
             decimal points and key gives a range of accuracy in metres. http://wiki.gis.com/wiki/index.php/Decimal_degrees

        This helper rounds off decimal places from extremely long latitude and longitude coordinates. The exact precision
        is defined by the meter accuracy variable passed by the user
        """
        accuracy_converter = {
                              (5000, 14999): 1,
                              (500, 4999): 2,
                              (50, 499): 3,
                              (5, 49): 4,
                              (0, 4): 5
                             }
        for (k1, k2) in accuracy_converter:
            if k1 <= accuracy <= k2:
                self.accuracy = accuracy_converter[(k1, k2)]

    def process(self, datum):
        """
        :param datum - (lat, long) tuple of Latitude and Longitude values
        """
        yield round(datum[0], self.accuracy), round(datum[1], self.accuracy)


class AnonymizeAddressBlock(beam.DoFn, ABC):
    def __init__(self, accuracy=100):
        """
        :param accuracy - default 100, this variables determines the number of digits to be masked in the block number

        From a given address we extract the block number and street name. User specified number of trailing digits can be
        masked off from the block number to anonymize and hide sensitive information.

        example     input -> Address = 123 Main Street, Pittsburgh   Accuracy - 100
                    output -> 123, Main Street, 100  (depicting 100th block of main street)
        """
        self.accuracy = accuracy

    def process(self, datum):
        """
        :param datum - complete address along with street name and block number
        """
        block_num = re.findall(r"^[0-9]*", datum)

        # return the stripped number if present, else return empty string
        block_num = block_num[0] if block_num else ""

        street_name = re.findall(r"[^\d](.+?),", datum)

        # return the stripped street name if present, else return empty string
        street_name = street_name[0] if street_name else ""

        # anonymize block
        anon_block_num = (int(block_num) // self.accuracy) * self.accuracy

        yield block_num, street_name, anon_block_num


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


def unix_to_date_strings(unix_date):
    """
    this function converts unix timestamps (integer type) to human readable UTC and Eastern timestamps (string type)
    it is more robust than the above function and clears up ambiguity over what timezone the returned datetime is in
    :param unix_date: int
    :return: utc_conv, east_conv: strings
    """
    dt_object = datetime.fromtimestamp(unix_date)
    utc_conv = dt_object.astimezone(tz=pytz.utc)
    east_conv = dt_object.astimezone(tz=pytz.timezone('America/New_York'))
    return str(utc_conv), str(east_conv)


def id_underspecified_addresses(datum, self):
    """
    Identify whether a given street address, partitioned into street name, street number, and cross street name,
    is underspecified or not. An underspecified address is defined as any address that does not have an exact
    street number and is not an intersection. Examples of underspecified addresses are block numbers or
    ranges of addresses.
    :return: datum in PCollection (dict) with new field (address_type) identifying level of address specificity
    """
    if datum[self.street_name_field]:
        if datum[self.street_num_field].isnumeric():
            address_type = 'Precise'
        else:
            if not datum[self.street_num_field] and datum[self.cross_street_field]:
                address_type = 'Intersection'
            else:
                address_type = 'Underspecified'
    elif datum[self.lat_field] != 0.0 and datum[self.long_field] != 0.0:
        address_type = 'Coordinates Only'
    else:
        address_type = 'Missing'
    datum['address_type'] = address_type
    return datum


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


def gmap_geocode_address(datum, address_field):
    api_key = os.environ["GMAP_API_KEY"]
    base_url = "https://maps.googleapis.com/maps/api/geocode/json"

    coords = {'lat': None, 'long': None}
    address = datum[address_field]
    if 'pittsburgh' not in address.lower():
        address += ' pittsburgh'
    try:
        res = requests.get(f"{base_url}?address={address}&key={api_key}")
        results = res.json()['results'][0]
        if len(results):
            fmt_address = results['formatted_address']
            if fmt_address != 'Pittsburgh, PA, USA':
                api_coords = results['geometry']['location']
                coords['lat'] = float(api_coords.get('lat'))
                coords['long'] = float(api_coords.get('lng'))
        else:
            pass
    except requests.exceptions.RequestException as e:
        pass
    try:
        if fmt_address != 'Pittsburgh, PA, USA':
            datum[address_field] = fmt_address
        datum['lat'] = coords['lat']
        datum['long'] = coords['long']
    except TypeError:
        datum['lat'] = None
        datum['long'] = None

    return datum


def regularize_and_geocode_address(datum, self):
    """
    Take in addresses of different formats, regularize them to USPS/Google Maps format, then geocode lat/long values
    :return: datum in PCollection (dict) with two new fields (lat, long) containing coordinates
    """
    api_key = os.environ["GMAP_API_KEY"]
    base_url = "https://maps.googleapis.com/maps/api/geocode/json"
    if datum['address_type'] == 'Intersection':
        address = str(datum[self.street_name_field]) + ' and ' + str(datum[self.cross_street_field]) + ', ' + str(datum[self.city_field])
    # elif self.address_field in datum:
    elif not self.partioned_address:
        address = datum[self.address_field]
        datum['address_type'] = 'Precise'
    else:
        address = str(datum[self.street_num_field]) + ' ' + str(datum[self.street_name_field]) + ', ' + str(datum[self.city_field])
    if 'none' not in address.lower():
        datum['api_specified_address'] = address
    else:
        address = 'Pittsburgh, PA, USA'
    coords = {'lat': None, 'long': None}
    try:
        res = requests.get(f"{base_url}?address={address}&key={api_key}")
        if res.json()['results']:
            results = res.json()['results'][0]
            if len(results):
                fmt_address = results['formatted_address']
                api_coords = results['geometry']['location']
                if re.search(r'\bPA\b', fmt_address) and fmt_address != 'Pittsburgh, PA, USA':
                    datum['google_formatted_address'] = fmt_address
                    coords['lat'] = float(api_coords.get('lat'))
                    coords['long'] = float(api_coords.get('lng'))
                else:
                    datum['address_type'] = 'Unmappable'
    except requests.exceptions.RequestException as e:
        pass
    except KeyError:
        pass
    try:
        datum[self.lat_field] = coords['lat']
        datum[self.long_field] = coords['long']
    except TypeError:
        datum[self.lat_field] = None
        datum[self.long_field] = None

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
