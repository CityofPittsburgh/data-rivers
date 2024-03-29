from __future__ import absolute_import

import argparse
import logging
import re
import json
import os
import io
from json import JSONDecodeError

import pytz
import math
from datetime import datetime
import time
import requests
from abc import ABC
from avro import schema
from dateutil import parser
import numpy as np

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from google.cloud import bigquery, storage, dlp_v2

dt = datetime.now()
bq_client = bigquery.Client()
storage_client = storage.Client()

# current (as of 2024) format of full 16 char parcel numbers
# detailed by each components char length, whether letters are  permitted in that component, and if the component is
# required for conversion into a full 16 char string
# (some formats omit the final characters and these should be converted to zeros)
CITY_PARC_NUM_FORMAT = {
        "len"       : [4, 1, 5, 4, 2],
        "alpha_char": [False, True, False, True, True],
        "required"  : [True, True, True, False, False]
}


class JsonCoder(object):
    """A JSON coder interpreting each line as a JSON string."""

    def encode(self, x):
        return json.dumps(x)
      
    def decode(self, x):
        try:
            return json.loads(x)
        except JSONDecodeError:
            if "}{" in str(x):
                splits = []
                split_1, split_2 = str(x).split("}{")
                split_1 = split_1[2:len(split_1)] + "}"
                splits.append(split_1)
                split_2 = "{" + split_2[0:len(split_2)-1]
                splits.append(split_2)
                for dict in splits:
                    return JsonCoder.decode(self, dict)
            elif ":," in str(x):
                str_x = x.decode('utf-8')
                str_x = io.StringIO(str_x).getvalue()
                fixed = str_x.replace(':,', ': null,')
                return JsonCoder.decode(self, bytes(fixed, 'utf-8'))
            elif "\\'" or '\\"' in str(x):
                fixed = x
                str_x = x.decode('utf-8')
                str_x = io.StringIO(str_x).getvalue()
                if "\\'" in str_x:
                    fixed = str_x.replace("\\'", "'")
                elif '\\"' in str_x:
                    fixed = str_x.replace('\\"', '\"')
                return JsonCoder.decode(self, bytes(fixed, 'utf-8'))
            else:
                pass
    
    
class AnonymizeAddressBlock(beam.DoFn, ABC):
    def __init__(self, anon_vals):
        """
        :param anon_vals - Tuple of (field_name, accuracy) where accuracy determines the number of digits to be masked
                           in the block number. From a given address we extract the block number . User
                           specified number of trailing digits can be masked off from the block number to anonymize
                           and hide sensitive information.

        example     input -> Address = 123 Main Street, Pittsburgh   Accuracy - 100
                    output -> 123, Main Street, 100  (depicting 100th block of main street)
        """
        self.anon_values = anon_vals

    def process(self, datum):
        """
        :param datum - complete address along with street name and block number
        """
        for (field, accuracy) in self.anon_values:
            address = datum[field]
            new_field_name = field.replace("pii_", "anon_")
            if address:
                block_num = re.findall(r"^[0-9]*", address)
                # return the stripped number if present, else return empty string
                block_num = block_num[0] if block_num else ""

                # anonymize block
                # Replace the field in datum with the masked values
                if block_num:
                    anon_block_num = str((int(block_num) // accuracy) * accuracy)
                    num_zeros = str(accuracy).count('0')
                    anon_block_num = anon_block_num[:-num_zeros] + anon_block_num[-num_zeros:].replace('0', 'X')
                    datum[new_field_name] = re.sub(r"^[0-9]*", anon_block_num, address)
                else:
                    datum[new_field_name] = address
            else:
                datum[new_field_name] = None

        yield datum


class AnonymizeLatLong(beam.DoFn, ABC):
    def __init__(self, anon_val):
        """
        :param accuracy - desired meter accuracy of the lat-long coordinates after rounding the decimals. Default 200m

        var: accuracy_converter - Dictionary of Accuracy versus decimal places whose values represent the number of
             decimal points and key gives a range of accuracy in metres.
             http://wiki.gis.com/wiki/index.php/Decimal_degrees

        This helper rounds off decimal places from extremely long latitude and longitude coordinates. The exact
        precisionis defined by the meter accuracy variable passed by the user
        """
        self.anon_values = anon_val
        self.accuracy_converter = {
                (5000, 14999): 1,
                (500, 4999)  : 2,
                (50, 499)    : 3,
                (5, 49)      : 4,
                (0, 4)       : 5
        }

    def process(self, datum):
        """
        :param datum - (lat, long) tuple of Latitude and Longitude values
        """
        for (lat, long, accuracy) in self.anon_values:
            for (k1, k2) in self.accuracy_converter:
                if k1 <= accuracy <= k2:
                    acc = self.accuracy_converter[(k1, k2)]
            try:
                datum[lat.replace("pii_", "anon_")] = str(round(float(datum[lat]), acc)) if datum[lat] else None
                datum[long.replace("pii_", "anon_")] = str(round(float(datum[long]), acc)) if datum[long] else None
                datum[lat] = str(datum[lat]) if datum[lat] else None
                datum[long] = str(datum[long]) if datum[long] else None
            except KeyError:
                pass

        yield datum


class ChangeDataTypes(beam.DoFn, ABC):
    def __init__(self, type_changes):
        """
        :param type_changes: list of tuples; each tuple consists of the field we want to change and the new data
        type we want for its value
        """
        self.type_changes = type_changes

    def process(self, datum):
        try:
            # if the value is to be converted to int or float but is already NaN then make it None
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
                    elif type_change[1] == "posint":
                        if int(datum[type_change[0]]) < 0 or int(datum[type_change[0]]) > 2147483647:
                            datum[type_change[0]] = None
                        else:
                            datum[type_change[0]] = int(abs(datum[type_change[0]]))
                    elif type_change[1] == "str":
                        if str(datum[type_change[0]]) != 'None':
                            datum[type_change[0]] = str(datum[type_change[0]])
                    elif type_change[1] == "nullstr":
                        if str(datum[type_change[0]]) != 'None':
                            if str(datum[type_change[0]]) == '':
                                datum[type_change[0]] = None
                            else:
                                datum[type_change[0]] = str(datum[type_change[0]])
                    elif type_change[1] == "bool":
                        try:
                            datum[type_change[0]] = json.loads(datum[type_change[0]].lower())
                        except AttributeError:
                            datum[type_change[0]] = datum[type_change[0]]
                except ValueError:
                    datum[type_change[0]] = None
                except TypeError:
                    datum[type_change[0]] = None
        except TypeError:
            pass

        yield datum


class ColumnsCamelToSnakeCase(beam.DoFn):
    def __init__(self, strip_field=''):
        """
        :param strip_field: optional string that indicates a substring that will be stripped from input
        in addition to converting string to snake case
        """
        self.strip_field = strip_field
    def process(self, datum):
        cleaned_datum = {camel_to_snake_case(k, self.strip_field): v for k, v in datum.items()}
        yield cleaned_datum


class ColumnsToLowerCase(beam.DoFn, ABC):
    def process(self, datum):
        cleaned_datum = {k.lower(): v for k, v in datum.items()}
        yield cleaned_datum


class ConvertBooleans(beam.DoFn, ABC):
    def __init__(self, bool_changes, include_defaults):
        """
        :param bool_changes: list of tuples; each tuple consists of the field we want to change, the value
        representing True, value representing False (thus, custom values can be targeted), and finally an indicator of
        how to treat missing values (e.g. make them all False). The tuples are set for all fields invidually to allow
        multiple sets of targets and missing value solutions.
        :param include_defaults: boolean to indicate if a list of common (default) values should also be taken into
        account. This allows unexpected values to be taken into account.
        """

        self.bool_changes = bool_changes
        self.include_defaults = include_defaults

    def process(self, datum):
        try:
            for val in self.bool_changes:
                if self.include_defaults:
                    t_vals = ["yes", "y", "t", "true", "1", "positive", str(val[1].lower())]
                    f_vals = ["no", "n", "f", "false", "0", "negative", str(val[2].lower())]

                else:
                    t_vals = str(val[1].lower())
                    f_vals = str(val[2].lower())

                try:
                    if not datum[val[0]]:
                        datum[val[0]] = val[3]
                    elif str(datum[val[0]]).lower() in t_vals:
                        datum[val[0]] = True
                    elif str(datum[val[0]]).lower() in f_vals:
                        datum[val[0]] = False

                except ValueError:
                    datum[val[0]] = val[3]
        except TypeError:
            pass
        yield datum


class ConvertGeography(beam.DoFn):
    def __init__(self, geo_field, geo_type=''):
        """
        :param geo_field - Name of field that will be converted into string formatted for BQ geography conversion
        :param geo_field - BQ geography datatype that string will be converted to
        """
        self.geo_field = geo_field
        self.geo_type = geo_type
    def process(self, datum):
        if datum[self.geo_field]:
            coord_list = datum[self.geo_field][datum[self.geo_field].find("[{")+2:datum[self.geo_field].find("}]")].split('}, {')
            formatted_geo = ''
            i = 1
            for coord in coord_list:
                lat_lng = coord.split(', ')
                rev_str = lat_lng[1].split(': ')[1] + " " + lat_lng[0].split(': ')[1]
                if i < len(coord_list):
                    formatted_geo += rev_str + ", "
                else:
                    formatted_geo += rev_str
                i += 1
            if self.geo_type:
                if 'POLYGON' in self.geo_type:
                    datum[self.geo_field] = f'{self.geo_type}(({formatted_geo}))'
                else:
                    datum[self.geo_field] = f'{self.geo_type}({formatted_geo})'
            else:
                datum[self.geo_field] = formatted_geo
        else:
            datum[self.geo_field] = None

        yield datum


class ConvertStringCase(beam.DoFn, ABC):
    def __init__(self, str_changes):
        """
        :param str_changes: list of tuples; each tuple consists of the field we want to change and the string format
        we want (upper, lower, sentence case, etc)
        """

        self.str_changes = str_changes

    def process(self, datum):
        for val in self.str_changes:
            if datum[val[0]] is not None:
                if val[1] == "upper":
                    datum[val[0]] = datum[val[0]].upper()
                elif val[1] == "lower":
                    datum[val[0]] = datum[val[0]].lower()
                elif val[1] == "sentence":
                    datum[val[0]] = datum[val[0]].sentence()
                elif val[1] == "title":
                    datum[val[0]] = datum[val[0]].title()
                elif val[1] == "capitalize":
                    datum[val[0]] = datum[val[0]].capitalize()

        yield datum


class ExtractField(beam.DoFn):
    def __init__(self, source_fields, nested_fields, new_field_names, additional_nested_fields):
        self.source_fields = source_fields
        self.nested_fields = nested_fields
        self.new_field_names = new_field_names
        self.additional_nested_fields = additional_nested_fields

    def process(self, datum):
        if datum is not None:
            for src, nst, new, anf in zip(self.source_fields, self.nested_fields,
                                          self.new_field_names, self.additional_nested_fields):
                datum = extract_field(datum, src, nst, new, anf)
        else:
            logging.info('got NoneType datum')

        yield datum


class ExtractFieldWithComplexity(beam.DoFn):
    def __init__(self, source_fields, nested_fields, new_field_names, additional_nested_fields="", search_fields="", additional_search_vals=""):
        self.source_fields = source_fields
        self.nested_fields = nested_fields
        self.new_field_names = new_field_names
        self.additional_nested_fields = additional_nested_fields
        self.search_fields = search_fields
        self.additional_search_vals = additional_search_vals

    def process(self, datum):
        if datum is not None:
            for src, nst, new, anf, sch, asv in zip(self.source_fields, self.nested_fields, self.new_field_names,
                                                    self.additional_nested_fields, self.search_fields, self.additional_search_vals):
                datum = extract_field(datum, src, nst, new, anf, sch, asv)
        else:
            logging.info('got NoneType datum')

        yield datum


class FilterOutliers(beam.DoFn, ABC):
    def __init__(self, outlier_check):
        """
        :param
        """
        self.outlier_check = outlier_check

    def process(self, datum):

        try:
            # if the value is to be converted to int or float but is already NaN then make it None
            for oc in self.outlier_check:
                if datum[oc[0]] < oc[1] or datum[oc[0]] > oc[2]:
                    datum[oc[0]] = None
        except TypeError:
            pass

        yield datum


class FilterFields(beam.DoFn, ABC):
    def __init__(self, target_fields, exclude_target_fields = True):
        self.target_fields = target_fields
        self.exclude_target_fields = exclude_target_fields

    def process(self, datum):
        if datum is not None:
            datum = filter_fields(datum, self.target_fields, self.exclude_target_fields)

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


class FormatAndClassifyAddress(beam.DoFn, ABC):
    def __init__(self, loc_field_names, contains_pii):
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
        self.street_num_field = loc_field_names["street_num_field"]
        self.street_name_field = loc_field_names["street_name_field"]
        self.cross_street_field = loc_field_names["cross_street_field"]
        self.city_field = loc_field_names["city_field"]
        self.lat_field = loc_field_names["lat_field"]
        self.long_field = loc_field_names["long_field"]

        self.pii_vals = contains_pii

    def process(self, datum):
        if datum[self.lat_field] and datum[self.long_field]:
            datum[self.lat_field] = float(datum[self.lat_field])
            datum[self.long_field] = float(datum[self.long_field])
        else:
            datum[self.lat_field] = float(0.0)
            datum[self.long_field] = float(0.0)

        datum['address_type'] = None
        datum = id_underspecified_addresses(datum, self)

        yield datum


class GetValsFromExternalFile(beam.DoFn, ABC):
    def __init__(self, file_name, source_field, update_field):
        self.file_name = file_name
        self.source_field = source_field
        self.update_field = update_field
        client = storage.Client()
        cw_bucket = client.get_bucket("user_defined_data")
        cw_blob = cw_bucket.get_blob(self.file_name)
        cw = cw_blob.download_as_string()
        self.crosswalk_dict = json.loads(cw.decode('utf-8'))

    def process(self, datum):
        try:
            datum[self.update_field]
        except KeyError:
            datum[self.update_field] = datum[self.source_field]

        if datum[self.source_field] in self.crosswalk_dict:
            try:
                datum[self.update_field] = self.crosswalk_dict[datum[self.source_field]]
            except Exception as e:
                print(e)
        elif not datum[self.source_field]:
            datum[self.source_field] = datum[self.update_field]
        else:
            if datum[self.source_field] not in str(self.crosswalk_dict):
                print(f"Untracked value found in {self.source_field}: {datum[self.source_field]}")

        yield datum


class GoogleMapsGeocodeAddress(beam.DoFn, ABC):
    def __init__(self, key, loc_field_names, del_org_input):
        """
        :param key: string containing the Google Maps API key, extracted from an environmental variable
        :param loc_field_names: dictionary of 3 field name keys that contain the following information:
            :param address_field: name of field that contains single-line addresses
            :param lat_field: name of field that contains the latitude of an address
            :param long_field: name of field that contains the longitude of an address
        :param del_org_input: boolean that determines if the original address and lat/long fields will be preserved
        """
        self.address_field = loc_field_names["address_field"]
        self.lat_field = loc_field_names["lat_field"]
        self.long_field = loc_field_names["long_field"]

        self.api_key = key
        self.del_org_input = del_org_input

    def process(self, datum):
        if "pii" in self.address_field:
            formatted_name = "pii_google_formatted_address"
        else:
            formatted_name = "google_formatted_address"

        datum[formatted_name] = None

        if datum['address_type'] != 'Missing':
            datum = regularize_and_geocode_address(datum, self, formatted_name, self.del_org_input)
        if self.del_org_input:
            datum.pop(self.address_field)
        yield datum


class GeocodeAddress(beam.DoFn, ABC):

    def __init__(self, address_field):
        self.address_field = address_field

    def process(self, datum):
        geocode_address(datum, self.address_field)

        yield datum


class PrependCharacters(beam.DoFn, ABC):
    def __init__(self, input_field, length, char='0', check_numeric=False):
        self.input_field = input_field
        self.length = length
        self.char = char
        self.check_numeric = check_numeric

    def process(self, datum):
        if datum[self.input_field]:
            id_str = str(datum[self.input_field])
            if id_str not in ('nan', 'None', 'null'):
                if self.check_numeric:
                    if id_str.isnumeric():
                        while len(id_str) < self.length:
                            id_str = self.char + id_str
                else:
                    while len(id_str) < self.length:
                        id_str = self.char + id_str
            datum[self.input_field] = id_str

        yield datum


class ReformatPhoneNumbers(beam.DoFn, ABC):
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


class ReplacePII(beam.DoFn):
    """
    Comments must be scrubbed for PII from 311 requests.
    Comments do not follow strict formatting so this is an imperfect approximation.
    Steps: extract fields, detect person names that are followed by hotwords for exclusion (e.g. park or street),
    place an underscore between the detected words to prevent accidental redaction, redact PII
    The Google data loss prevention (dlp) API is used (via helper function) to scrub PII
    """
    def __init__(self, input_field, new_field_name, retain_location: bool, info_types, gcloud_project, place_id_bucket):
        self.input_field = input_field
        self.new_field_name = new_field_name
        self.retain_location = retain_location
        self.info_types = info_types
        self.gcloud_project = gcloud_project
        self.place_id_bucket = place_id_bucket

    def process(self, datum):
        print(F"replacing/scanning PII of {datum['id']}")
        print(F"{datum}")
        if datum is not None:
            datum[self.new_field_name] = replace_pii(datum, self.input_field, self.retain_location, self.info_types,
                                                     self.gcloud_project, self.place_id_bucket)
            yield datum
        else:
            logging.info('got NoneType datum')


class StandardizeParcelNumbers(beam.DoFn, ABC):
    """
    :param parc_fd: a string containing the parcel number field in the source data. NOTE: a function from calling
    script extracts this information. The source data's structure can vary depending on data source. The most
    efficient approach is to extract the parcel number, if necessary, in another function/script
    """
    def __init__(self, parc_fd):
        self.parc_fd = parc_fd

    # the func called below is also used by frameworks other than BEAM/Dataflow. Thus, parc_fd is passed in as a
    # string directly from self, as opposed to passing in self and extracting in the called func, consistent with
    # other approaches in these utilites.
    def process(self, datum):
        if datum[self.parc_fd] is None:
            datum[self.parc_fd] = "invalid_input"
            yield datum
        
        datum[self.parc_fd] = standardize_parc_num(self.parc_fd, datum)

        yield datum


class StandardizeTimes(beam.DoFn, ABC):
    def __init__(self, time_changes, t_format = "%m/%d/%Y %H:%M:%S%z"):
        """
        :param time_changes: list of tuples; each tuple consists of an existing field name containing date strings +
        the name of the timezone the given date string belongs to.
        The function takes in date string values and standardizes them to datetimes in UTC, Eastern, and Unix.
        formats. It is powerful enough to handle datetimes in a variety of timezones and string formats.
        The user must provide a timezone name contained within pytz.all_timezones.
        As of June 2021, a list of accepted timezones can be found on
        https://en.wikipedia.org/wiki/List_of_tz_database_time_zones#List
        Please note that the timezone names are subject to change and the code would have to be updated accordingly.
        (JF)
        """
        self.time_changes = time_changes
        self.t_format = t_format

    def process(self, datum):

        for time_change in self.time_changes:
            if datum[time_change[0]] is not None and datum[time_change[0]] != '':
                parse_dt = parser.parse(datum[time_change[0]])
                clean_dt = parse_dt.replace(tzinfo=None)
                try:
                    pytz.all_timezones.index(time_change[1])
                except ValueError:
                    pass

                else:
                    loc_time = pytz.timezone(time_change[1]).localize(clean_dt, is_dst=None)
                    utc_conv = loc_time.astimezone(tz=pytz.utc)
                    east_conv = loc_time.astimezone(tz=pytz.timezone('US/Eastern'))
                    unix_conv = utc_conv.timestamp()
                    datum.update({'{}_UTC'.format(time_change[0]) : utc_conv.strftime(self.t_format),
                                  '{}_EST'.format(time_change[0]) : east_conv.strftime(self.t_format),
                                  '{}_UNIX'.format(time_change[0]): int(unix_conv)})

            else:
                datum.update({'{}_UTC'.format(time_change[0]): None,
                              '{}_EST'.format(time_change[0]): None,
                              '{}_UNIX'.format(time_change[0]): None})

        yield datum


class StripBeforeDelim(beam.DoFn, ABC):
    def __init__(self, strip_fields, delim, before_or_after):
        """
        :param strip_fields: list of field names; each val describes a string field that has unwanted data
        on one side of the delimiter
        :param delim: list of characters that splits the provided field in two
        :param before_or_after: list of binary integers that indicates which side of the delimiter should be retained
        """
        self.strip_fields = strip_fields
        self.delim = delim
        self.before_or_after = before_or_after

    def process(self, datum):
        for i in range(0, len(self.strip_fields)):
            try:
                if datum[self.strip_fields[i]]:
                    datum[self.strip_fields[i]] = datum[self.strip_fields[i]].split(self.delim[i])[self.before_or_after[i]]
            except KeyError:
                print(f"Field {self.strip_fields[i]} not found in datum")
            except IndexError as i_e:
                print(i_e)
                datum[self.strip_fields[i]] = None
        yield datum


class StripStrings(beam.DoFn, ABC):
    def __init__(self, fields):
        """
        :param input: list of field names; each val describes a field in which the strings will have leading and
        trailing white space stripped
        """
        self.fields = fields

    def process(self, datum):
        for val in self.fields:
            if datum[val] is not None:
                datum[val] = datum[val].strip()

        yield datum


class SwapFieldNames(beam.DoFn, ABC):
    def __init__(self, name_changes):
        """:param name_changes: list of tuples consisting of existing field name + name to which it should be changed"""
        self.name_changes = name_changes

    def process(self, datum):

        for name_change in self.name_changes:
            try:
                datum[name_change[1]] = datum[name_change[0]]
                del datum[name_change[0]]
            except TypeError:
                print(f"{name_change[0]} and {name_change[1]} were not both found within datum")
                datum[name_change[1]] = None
            except KeyError:
                print(f"{name_change[0]} not found as a field within datum")
                datum[name_change[1]] = None

        yield datum


def generate_args(job_name, bucket, argv, schema_name, default_arguments, limit_workers = [False, None], backfill_dag
= False, use_df_runner = False):
    """
    generate arguments for DataFlow jobs (invoked in DataFlow scripts prior to execution). In brief, this function
    initializes the basic options and setup for each step in a dataflow pipeline(e.g. the GCP project to operate on,
    the subnet to run the job, etc). The function also bundles the runtime vars (e.g. buckets, schema names,
    etc) in with this initialized information via the parser. The bundled together vars are ultimately returned as
    known_args (which are the runtime vars passed into Beam, e.g. avro_output and input), and pipeline_options (which
    determine how Beam operates and where it is pointed to etc) as Protected Attributes (_flags).

    :param job_name: name for DataFlow job (string)
    :param bucket: Google Cloud Storage bucket to which avro files will be uploaded (string)
    :param argv: arg parser object (this will always be passed as 'argv=argv' in DataFlow scripts)
    :param schema_name: Name of avro schema file in Google Cloud Storage against which datums will be validated
    :param limit_workers: list with boolean variable in first position determining if the max number of dataflow
    workers should be limited (to comply with quotas) and the corresponding max number of workers in the second position
    :param backfill_dag boolean indicating if the dataflow job is part of a backfill DAG. Backfill schemas are stored in
    a sub directory and this will instruct the util function to append this to the file path for schema retrieval
    :param use_df_runner (boolean) that is used to instruct the dataflow pipeline to run in either the VMs
    instantiated in the dataflow API (True) or directly on the Airflow host machines (False)
    :return: known_args (arg parser values), Beam PipelineOptions instance, avro_schemas stored as dict in memory

    If you're doing local development on your laptop, and you'd like to execute the dataflow pipeline on your
    own machine, specify the DirectRunner as a runtime arg
    For example: --runner='DirectRunner' (e.g.
    python qalert_activities_dataflow.py --input gs://pghpa_test_qalert/activities/2020/09/2020-09-23_activities.json
    --avro_output gs://pghpa_test_qalert/activities/avro_output/2020/09/2020-09-23/ --runner DirectRunner)

    If you want to use the GCP dataflow runner while testing code from your laptop, you can specify the
    DataflowRunner. It is smart to make sure the code runs in GCP dataflow before deploying the code. There have been a
    few rare instances where we've found that the direct runner on your laptop will run, but the same code doesn't
    run in GCP dataflow.

    When the pipeline is executing in Cloud Composer, the Airflow host that is used by Composer will either run the
    code "directly" or in Dataflow. The former means that the VMs that run Airflow will execute the dataflow
    pipeline. In this sense, because the code is run "directly", the Airflow hosts will not pass the job off GCP
    Dataflow. As of July 2023, we do not use any persistent VMs to host dataflow so submitting a dataflow job
    requires a new machine to be instantiating. This can take several minutes. If a dataset is large and/or the
    computations are complex, it may be most efficient to use the Dataflow Runner. However, if the dataset is smaller
    it is probably much faster to use the DirectRunner on an Airflow Host (the VMs). As of July 2023 we will switch
    to this approach. Tests have revealed that this is easily 10X faster.

    The DataflowRunner will be specified by default (see input args above). Within the dataflow script itself,
    the direct runner can be used by setting the boolean variable to False. As of July 2023, we have no plans to use
    other runners (e.g. Spark). This code would not take that approach into account.

    There are at least 2 ways to change the runner in theory. You can pass this in as a runtime argument in the BASH
    call that is executed by Airflow, which is the earliest point in the processing chain. Since our default plan
    going forward (as of July 2023) is to use the Dataflow Runner only when job complexity makes the instantiation
    time worthwhile, we will make this the default approach. Thus, the runner is specified in this utility function
    and can be changed to the Dataflow runner in the call that is made to this function from the dataflow script.

    """
    parser = argparse.ArgumentParser()

    parser.add_argument('--input', dest = 'input', required = True)
    parser.add_argument('--avro_output', dest = 'avro_output', required = True)
    parser.add_argument('--specify_runner', dest = 'spec_runner', required = False)
    args = vars(parser.parse_args())

    if use_df_runner or args['spec_runner'] == 'DataflowRunner':
        print('---------------------------------------------------')
        print('Using DataflowRunner')
        print('---------------------------------------------------')
        parser.add_argument('--runner', '-r', dest = 'runner', default = 'DataflowRunner', required = False)
    else:
        parser.add_argument('--runner', '-r', dest = 'runner', default = 'DirectRunner', required = False)


    known_args, pipeline_args = parser.parse_known_args(argv)

    arguments = default_arguments
    arguments.append('--job_name={}'.format(job_name))
    arguments.append('--staging_location=gs://{}/beam_output/staging'.format(bucket))
    arguments.append('--temp_location=gs://{}/beam_output/temp'.format(bucket))
    arguments.append('--runner={}'.format(vars(known_args)['runner']))
    arguments.append('--setup_file={}'.format(os.environ['SETUP_PY_DATAFLOW']))
    # ^this doesn't work when added to DEFAULT_DATFLOW_ARGS, for reasons unclear

    if limit_workers[0]:
        arguments.append(f"--max_num_workers={limit_workers[1]}")

    if backfill_dag:
        backfill_dir_path = "backfills/"
    else:
        backfill_dir_path = ""

    pipeline_args.extend(arguments)
    pipeline_options = PipelineOptions(pipeline_args)

    avro_schema = get_schema(schema_name, backfill_dir_path)

    return known_args, pipeline_options, avro_schema


# monkey patch for avro schema has  hing bug: https://issues.apache.org/jira/browse/AVRO-1737
def hash_func(self):
    return hash(str(self))


schema.RecordSchema.__hash__ = hash_func


# def download_schema(bucket_name, source_blob_name, destination_file_name):
#     """Downloads avro schema from Cloud Storage"""
#     bucket = storage_client.get_bucket(bucket_name)
#     blob = bucket.blob(source_blob_name)
#
#     blob.download_to_filename(destination_file_name)


def get_schema(schema_name, backfill_dir):
    """Read avsc from cloud storage and return json object stored in memory"""
    bucket = storage_client.get_bucket(F"{os.environ['GCS_PREFIX']}_avro_schemas")
    blob = bucket.get_blob('{}{}.avsc'.format(backfill_dir, schema_name))
    schema_string = blob.download_as_string()
    return json.loads(schema_string)


def camel_to_snake_case(val, strip_field=''):
    if strip_field:
        val = val.replace(strip_field, '')
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
    utc_conv = dt_object.astimezone(tz = pytz.utc)
    east_conv = dt_object.astimezone(tz = pytz.timezone('America/New_York'))
    return str(utc_conv), str(east_conv)


# Utilizes Allegheny county geocoding API
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


# Utilizes Google Maps API with fully specified address (i.e. not addresses that are broken into seperate fields like
# street name and street number)
# This function is not currently in use, but will be used in the future
def gmap_geocode_address(datum, address_field, self):
    base_url = "https://maps.googleapis.com/maps/api/geocode/json"

    coords = {'lat': None, 'long': None}
    address = datum[address_field]
    if 'pittsburgh' not in address.lower():
        address += ' pittsburgh'
    try:
        res = requests.get(f"{base_url}?address={address}&key={self.api_key}")
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


# This function determines what type of information is contained in an address and used by
# GoogleMapsClassifyAndGeocode()
def id_underspecified_addresses(datum, self):
    """
    Concatenate partitioned addresses (broken into street name, street number, and cross street name) into a
    single-line address, and identify whether a given street address is underspecified or not. An underspecified address
    is defined as any address that does not have an exact street number and is not an intersection.
    Examples of underspecified addresses are block numbers or ranges of addresses.
    :return: datum in PCollection (dict) with two new fields: address_type, which identifies level of address specificity,
    and input_name (pii_input_address or input_address) that contains a concatenated single-line address made up of
    street number, street name, and city name
    """
    if self.pii_vals:
        input_name = 'pii_input_address'
    else:
        input_name = 'input_address'
    address = None

    if datum[self.street_name_field]:
        address = str(datum[self.street_num_field]) + ' ' + str(datum[self.street_name_field]) + \
                  ', ' + str(datum[self.city_field])
        if datum[self.street_num_field].isnumeric():
            address_type = 'Precise'
        else:
            if not datum[self.street_num_field] and datum[self.cross_street_field]:
                address_type = 'Intersection'
                address = str(datum[self.street_name_field]) + ' and ' + \
                          str(datum[self.cross_street_field]) + ', ' + \
                          str(datum[self.city_field])
            else:
                address_type = 'Underspecified'
    elif datum[self.lat_field] != 0.0 and datum[self.long_field] != 0.0:
        address_type = 'Coordinates Only'
        datum[self.lat_field] = str(datum[self.lat_field])
        datum[self.long_field] = str(datum[self.long_field])
    else:
        address_type = 'Missing'

    datum[input_name] = address
    datum['address_type'] = address_type
    return datum


# This functions geocodes an address using Google Maps AND standardizes the address formatting
def regularize_and_geocode_address(datum, self, f_name, del_org_input):
    """
    Take in addresses of different formats, regularize them to USPS/Google Maps format, then geocode lat/long values
    :return: datum in PCollection (dict) with two new fields (lat, long) containing coordinates
    """
    base_url = "https://maps.googleapis.com/maps/api/geocode/json"

    if not datum[self.address_field]:
        address = 'Pittsburgh, PA, USA'
    elif 'none' in datum[self.address_field].lower():
        address = 'Pittsburgh, PA, USA'
    else:
        address = datum[self.address_field]

    coords = {'lat': None, 'long': None}

    # set delays for exponential backoff (prevents exceeding API quota) and is only used if API call fails
    curr_delay = 0.1
    max_delay = 10
    attempt_ct = 1

    if datum['address_type'] != 'Coordinates Only' and json.loads(os.environ['USE_PROD_RESOURCES'].lower()):
        # run until results are retrieved from API or exponential backoff reaches limit
        while curr_delay <= max_delay:
            res = requests.get(f"{base_url}?address={address}&key={self.api_key}")

            # if API returned results
            if res.json()['results']:
                results = res.json()['results'][0]

                # if results are not empty
                if len(results):
                    fmt_address = results['formatted_address']
                    api_coords = results['geometry']['location']

                    # if data could be mapped to PGH (if the formatted address is simply the city/state/country then API
                    # could not find a good result
                    if re.search(r'\bPA\b', fmt_address) and fmt_address != 'Pittsburgh, PA, USA':
                        datum[f_name] = fmt_address
                        coords['lat'] = str(api_coords.get('lat'))
                        coords['long'] = str(api_coords.get('lng'))
                    else:
                        datum['address_type'] = 'Unmappable'

                    # break here (irrespective of output) because the API returned a result
                    break

            # elif not res.json()['results'] and curr_delay < max_delay:
            else:
                time.sleep(curr_delay)
                curr_delay *= 2

                # all other conditions trigger break and are noted in data
                if curr_delay > max_delay:
                    datum[f_name] = f"API not accessible after {attempt_ct} attempts"
                    break

                # increment count for reporting
                attempt_ct += 1

    # update the lat/long (potentially overwriting the input, depending on what was passed in)
    google_lat_field = self.lat_field
    google_long_field = self.long_field
    if not del_org_input:
        org_lat_field = "input_" + self.lat_field
        org_long_field = "input_" + self.long_field
        google_lat_field = "google_" + self.lat_field
        google_long_field = "google_" + self.long_field
        datum[org_lat_field] = str(datum[self.lat_field])
        datum[org_long_field] = str(datum[self.long_field])
        datum.pop(self.lat_field)
        datum.pop(self.long_field)
    try:
        datum[google_lat_field] = coords['lat']
        datum[google_long_field] = coords['long']
    except TypeError:
        datum[google_lat_field] = None
        datum[google_long_field] = None

    return datum


def extract_field(datum, source_field, nested_field, new_field_name, additional_nested_field="", search_field="", additional_search_val=""):
    """
    In cases where datum contains nested dicts, traverse to nested dict and extract a value for reassignment to a new
    non-nested field

    :param datum: datum in PCollection (dict)
    :param source_field: name of field containing desired nested value (str)
    :param nested_field: name of field nested within source_field dict the value of which we want to extract
    and assign its value to new_field_name (str)
    :param new_field_name: name for new field we're creating with the value of nested_field (str)
    :param additional_nested_field: optional field; name of field nested within nested_field dict the value of which we
    want to extract and assign its value to new_field_name. Some datums have deeper levels of nested data than others
    (str)
    :param search_field: optional field; nested field within source_field whose value indicates which dict within a list
    of dicts should be extracted and returned within new_field_name. Can either be a string that id's a field name
    that should *not* be present within list, or a key/value pair with a specific value we should be looking for
    (str, dict)
    :param additional_search_val: optional field; nested field within nested_field whose value indicates which dict
    within a list of dicts should be extracted and returned within new_field_name. Some datums have deeper levels of
    nested data than others (str)
    :return: datum in PCollection (dict)
    """
    try:
        # evaluate how many layers deep nested dict that contains our desired data is
        if additional_nested_field:
            if datum[source_field][nested_field][additional_nested_field]:
                datum[new_field_name] = str(datum[source_field][nested_field][additional_nested_field])
            else:
                datum[new_field_name] = None
        else:
            if datum[source_field][nested_field]:
                datum[new_field_name] = str(datum[source_field][nested_field])
            else:
                datum[new_field_name] = None
    except TypeError:
        # sometimes nested dicts are actually lists of dicts, in which case we need to use search_field to decide
        # which value to retrieve
        if search_field:
            try:
                # evaluate if search_field is a key/value pair or a field name that indicates an unwanted record
                if type(search_field) is dict:
                    search_key = list(search_field)[0]
                    # loop through list of dicts to find index of dict that has desired value
                    correct_index = next((i for (i, d) in enumerate(datum[source_field]) if d[search_key]==search_field[search_key]), None)
                else:
                    # loop through list of dicts to find index of dict that does not have undesired field
                    correct_index = next((i for (i, d) in enumerate(datum[source_field]) if search_field not in d), None)
            # error is thrown if a deeper-level search needs to be performed; this block handles that case
            except TypeError:
                correct_index = next((i for (i, d) in enumerate(datum[source_field][nested_field]) if additional_search_val in d[additional_nested_field]), None)
            # if index number was successfully returned by search, call util function to pull the field from that index
            if correct_index is not None:
                datum = extract_field_from_nested_list(datum, source_field, correct_index, nested_field, new_field_name,
                                                       additional_nested_field)
            else:
                try:
                    # if for some reason the initial analysis of the search field didn't return an index number,
                    # return the index with the max value for the search field (e.g., the max/most recent date value)
                    recent_rec = max(datum[source_field], key=lambda x: x[search_field])
                    correct_index = next((i for (i, d) in enumerate(datum[source_field]) if d[search_field]==recent_rec[search_field]), None)
                    datum = extract_field_from_nested_list(datum, source_field, correct_index, nested_field, new_field_name,
                                                           additional_nested_field)
                except:
                    datum[new_field_name] = None

        else:
            datum[new_field_name] = None
    except KeyError:
        datum[new_field_name] = None

    return datum


def extract_field_from_nested_list(datum, source_field, list_index, nested_field, new_field_name, additional_nested_field="", additional_search_val=""):
    """
    In cases where datum contains values consisting of lists of dicts, isolate a nested dict within a list and extract
    a value for reassignment to a new non-nested field

    :param datum: datum in PCollection (dict)
    :param source_field: name of field containing desired nested value (str)
    :param list_index: index of relevant nested list contained within source_field (int)
    :param nested_field: name of field nested within the desired list of dicts contained within source_field (str)
    :param new_field_name: name for new field we're creating with the value of nested_field (str)
    :param additional_nested_field: optional field; name of field nested within nested_field dict the value of which we
    want to extract and assign its value to new_field_name. Some datums have deeper levels of nested data than others (str)
    :param additional_search_val: optional field; nested field within nested_field whose value indicates which dict
    within a list of dicts should be extracted and returned within new_field_name. Some datums have deeper levels of
    nested data than others (str)
    :return: datum in PCollection (dict)
    """
    try:
        # evaluate how many layers deep nested dict that contains our desired data is
        if additional_nested_field:
            datum[new_field_name] = datum[source_field][list_index][nested_field][additional_nested_field]
        else:
            datum[new_field_name] = datum[source_field][list_index][nested_field]
    except KeyError:
        try:
            # sometimes index number appears in the reverse order to what is expected (i.e., after the nested field
            # rather than before it). try extracting field again by reversing the order
            if additional_nested_field:
                datum[new_field_name] = datum[source_field][nested_field][list_index][additional_nested_field]
            else:
                datum[new_field_name] = datum[source_field][nested_field][list_index]
        except TypeError:
            datum[new_field_name] = None
    except TypeError:
        try:
            # attempt search on nested fields in case value still can't be retrieved
            correct_index = next((i for (i, d) in enumerate(datum[source_field][list_index][nested_field]) if additional_search_val in d[additional_nested_field]), None)
            datum[new_field_name] = datum[source_field][list_index][nested_field][correct_index][additional_nested_field]
        except:
            datum[new_field_name] = None
    except IndexError:
        datum[new_field_name] = None

    return datum


def filter_fields(datum, target_fields, exclude_target_fields = True):
    """
    :param datum: datum in PCollection (dict)
    :param target_fields: list of fields to drop or to preserve (dropping all others) (list)
    :param exclude_target_fields: preserve or drop relevant fields arg. we add this as an option because in some
    cases the list
    of fields we want to preserve is much longer than the list of those we want to drop, and vice verse, so having this
    option allows us to make the hard-coded target_fields arg in the dataflow script as terse as possible (bool)
    :return:
    """
    fields_for_deletion = []
    if exclude_target_fields:
        for k, v in datum.items():
            if k in target_fields:
                fields_for_deletion.append(k)
    else:
        for k, v in datum.items():
            if k not in target_fields:
                fields_for_deletion.append(k)

    for field in fields_for_deletion:
        if field in datum.keys():
            datum.pop(field, None)

    return datum


def replace_pii(datum, input_field, retain_location, info_types, gcloud_project, place_id_bucket):
    dlp_client = dlp_v2.DlpServiceClient()

    try:
        input_str = datum[input_field]
    except TypeError:
        print("Error extracting value from " + str(input_field) + " field")
        input_str = ""
    if not input_str:
        input_str = "No comment"
    if retain_location:
        input_str = snake_case_place_names(input_str, place_id_bucket)

    item = {"value": input_str}
    max_findings = 0
    include_quote = False
    inspect_config = {
        "info_types": info_types,
        "include_quote": include_quote,
        "limits": {"max_findings_per_request": max_findings},
    }
    deidentify_config = {
        "info_type_transformations": {
            "transformations": [
                {"primitive_transformation": {"replace_with_info_type_config": {}}}
            ]
        }
    }
    parent = "projects/{}".format(gcloud_project)

    curr_delay = 0.1
    max_delay = 10
    attempt_ct = 1

    # run until results are retrieved from API or exponential backoff reaches limit
    while curr_delay <= max_delay:
        try:
            response = dlp_client.deidentify_content(parent, deidentify_config, inspect_config, item)
        except TypeError:
            response = dlp_client.deidentify_content(
                request={
                    "parent": parent,
                    "deidentify_config": deidentify_config,
                    "inspect_config": inspect_config,
                    "item": item,
                }
            )
        if response.item.value:
            break
        # elif not response and curr_delay < max_delay:
        else:
            time.sleep(curr_delay)
            curr_delay *= 2

            # all other conditions trigger break and are noted in data
            if curr_delay > max_delay:
                print(f"Google DLP API not accessible after {attempt_ct} attempts")
                break

            # increment count for reporting
            attempt_ct += 1

    return response.item.value


def snake_case_place_names(input, place_id_bucket):
    # Helper function to take a pair of words, containing place name identifiers, and join them together (with an
    # underscore by default). This prevents NLP based Data Loss Prevention/PII scrubbers from targeting places for
    # name based redaction (e.g. avoiding redacting "Schenley" from "Schenley Park"), because the GCP tools will not
    # treat the joined phrase as person's name. This approach should be phased out after a less brittle and more elegant
    # tool is developed.

    bucket = storage_client.get_bucket(place_id_bucket)

    place_name_blob = bucket.blob('place_identifiers.txt')
    place_name_identifiers = place_name_blob.download_as_string().decode('utf-8')

    street_num_blob = bucket.blob('street_num_identifiers.txt')
    street_num_identifiers = street_num_blob.download_as_string().decode('utf-8')

    # if an identifier is found (indicative of a place such as a road or park), we want to join the place with the
    # preceding word with the join character. Thus, "Moore Park" would become "Moore_Park".
    joined_places = (re.sub(r'(\s)\b({})\b'.format(place_name_identifiers), r'_\2', input,
                            flags=re.IGNORECASE))
    joined_places = (re.sub(r'\b({})\b(\s)'.format(street_num_identifiers), r'\1_', joined_places,
                            flags=re.IGNORECASE))

    return joined_places


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


def standardize_parc_num(parc_fd, datum):
    """
       function to standardize the format of parcel numbers/block lots to the county format. this is designed to work
       in parallel processing applications (e.g. dataflow or pandas.apply etc)

       the terminology block/lot and parcel number are interchangeable

       the parcel number or block/lot of a property are defined in several ways, depending on the data source. it is
       best to consider the entire parcel number (a string) as consisting of multiple components. each components
       represents a different geographical entity (e.g. the block and lot, etc.). As of 11/23 there are 5 components
       to each parcel number, each with a unique length. the formatting may change, so the current format limitations
       are a constant declared at the top of this script. Here are some conventions to keep in mind:
       1) each of the 5 components of the parcel number string are fixed in length (4, 1, 5, 4, & 2)
       2) the only acceptable format is the same the county uses- a 16 char string (no special chars)
       3) various data sources (esp. the city's data sources) insert hyphens and ommit entire components if they are
       all zeros
       4) the 5th character must be a letter.
       5) letters may be present in the final 2 components of the parcel number. the exact character within these
       components varies.
       6) no characters can be omitted (e.g. 1234X12345123412 is correct)
       7) zero padding the parel number is very common in city data sources
       8) however, the city frequently inserts hyphens and does not zero pad them.
            AS AN EXAMPLE: 0001X00001000101 is the correctly formatted version of a block/lot that could appear as
       1-X-1-1-1, 0001-X-00001-0001-01)..

       This function will dehypenate, zero pad, and verify correctness

       :param input_parc: string input value representing the parcel number
       :return out: a string of the correctly formatted parcel number
       """

    # skip input anomolies
    try:
        parc_str_extract = datum[parc_fd]
        parc_str = parc_str_extract.strip().upper()
    except ValueError:
        # input not a string- return "invalid_input" so script keeps running
        print(type(parc_str_extract))
        return "invalid_input"
    except AttributeError:
        # most likely input is None. This SHOULD be caught in calling function and is a fail safe here
        print(type(parc_str_extract))
        return "invalid_input"

    # all values must be a hyphen or alphanumeric (no special chars)
    for char_val in parc_str:
        if not char_val.isalnum() and char_val != "-":
            return "invalid input"

    # break the parcel number into componets and place in a list called "parts". the strategy for breaking the parts
    # uses this conditional logic: either the string contains hyphens (and can be variable length) or it is exactly 16
    # chars.
    # break the hyphenated string into its component values and place in "parts". if there are too many components
    # (too many  hyphens) then it is invalid
    if parc_str.__contains__("-"):
        parts = parc_str.split("-")
        if len(parts) > len(CITY_PARC_NUM_FORMAT["required"]):
            return "invalid input"

    # if the input doesn't have hyphens (determined above) and is the proper length (must be 16 charts),
    # make a growing list (parts) with all 5 segments of the parc number
    # grab the leading character(s) and place them into parts (as seperate elements)
    # then drop those characters from the input so that only leading characters are extracted
    # the end result is each component of the input parc number is extracted and broken down for further analysis
    elif len(parc_str) == 16:
        parts = []
        for l in CITY_PARC_NUM_FORMAT["len"]:
            sel_string = parc_str[:l]
            parc_str = parc_str[l:]
            parts.append(sel_string)

    # fails above conditional logic (thus is not 16 chars long or does not contain hyphnens)
    else:
        return "invalid input"

    # all processing below assumes that the input is exactly 16 chars or hyphenated, and without special chars.
    # all fully inspected vals will overwrite the elements of conv_vals. The final 2 CITY_PARC_NUM_FORMAT of the
    # string are often ommitted by the city if they are all zeros. In this case, the zeros pre populated in conv_vals will
    # take their place
    conv_vals = ["", "", "", "0000", "00"]

    # for each part perform several checks below...
    for i in range(len(parts)):

        # verify all chars in component are correctly alpha or non alpha CITY_PARC_NUM_FORMAT (as of Nov 2023,
        # only components #1 and #3 cannot have a letter)
        # checks that each char within each component is correctly alpha or numeric (e.g not like "12A4" in
        # the first component):
        # for each char in the selected part (determined by 'i')
        for c_num in parts[i]:
            if not CITY_PARC_NUM_FORMAT["alpha_char"][i] and c_num.isalpha():
                return "invalid input"

        # verify component is not longer than allowed (if the string was 16 chars then this has to be correct,
        # so this only really is useful if the input was hyphenated. input string hyphen locations are variable)
        if len(parts[i]) > CITY_PARC_NUM_FORMAT["len"][i]:
            return "invalid input"

        # pad all string parts with zeros if they are shorter than required.
        # this code results in no changes if:
        # 1) the input string was 16 chars in length
        # OR
        # 2) the selected part is already the correct length
        conv_vals[i] = parts[i].rjust(CITY_PARC_NUM_FORMAT["len"][i], "0")

    # build the final parcel number
    out = "".join(conv_vals)

    # the final output must be 16 chars (this is essentially guaranteed by this point and this is a final
    # safeguard
    # to restrict errant testing values and junk data that are entered, we require that there must be a minimum
    # of 2 unique characters (one letter and one number).
    # The reality is that there should be approx 4-5 unique characters as the absolute minimum.
    # The multiple systems which these data can be extracted from do not constrain or restrict usage of junk
    # data for internal testing purposes etc. The minimum of 2  unique characters prevents test strings
    # like "0000A00000000000" from passing through.
    # This is, however, only a weak safeguard -> 1234X567891011 is probably a test value and it would pass
    # End users should understand that manual inspection may be required depending on the use case.
    if len(out) == 16 and len(set(out)) > 2:
        return out
    else:
        return "invalid input"
