from __future__ import absolute_import
from __future__ import division
# patches unittest.TestCase to be python3 compatible
import datetime
import os

import unittest
import numpy as np
import requests
import pytz

#import dataflow_utils
from af2_dags.dependencies.dataflow_scripts.dataflow_utils import dataflow_utils# as af2_dataflow_utils
from numpy import random


class TestDataflowUtils(unittest.TestCase):

    def test_columns_camel_to_snake_case(self):
        datum = {'exampleColumn': 'foo', 'anotherExample': 'bar'}
        expected = {'example_column': 'foo', 'another_example': 'bar'}
        ccsc = dataflow_utils.ColumnsCamelToSnakeCase()
        self.assertEqual(next(ccsc.process(datum)), expected)

    def test_columns_to_lower_case(self):
        datum = {'Example_Column': 'foo', 'anotherExample': 'bar', 'With a Space': 'foo'}
        expected = {'example_column': 'foo', 'anotherexample': 'bar', 'with a space': 'foo'}
        clc = dataflow_utils.ColumnsToLowerCase()
        self.assertEqual(next(clc.process(datum)), expected)

    def test_change_data_types(self):
        datum = {'count': '1', 'zip': 15213, 'temp': 72, 'day': 31.1, 'pos': -3019057200,
                 'bool1': 'TRUE', 'bool2': 1, 'nan_float': np.nan, 'nan_int': np.nan, 'nan_str': np.nan}
        type_changes = [("count", 'int'), ("zip", 'str'), ("temp", 'float'),
                        ("day", 'int'), ("pos", 'posint'), ("bool1", 'bool'), ("bool2", 'bool'),
                        ("nan_float", 'float'), ("nan_int", 'int'), ("nan_str", 'str')]
        expected = {'count': 1, 'zip': '15213', 'temp': 72.0, 'day': 31, 'pos': None,
                    'bool1': True, 'bool2': True, 'nan_float': None, 'nan_int': None, 'nan_str': None}
        cdt = dataflow_utils.ChangeDataTypes(type_changes)
        self.assertEqual(next(cdt.process(datum)), expected)

    def test_convert_booleans(self):
        datum = {'bool_1': 'yeah', 'bool_2': 'nope',
                 'bool_3': 'nah', 'bool_4': 'yup',
                 'bool_5': '', 'bool_6': None}
        bool_changes = [('bool_1', 'yeah', 'nah', 'N/A'),
                        ('bool_2', 'yup', 'nope', False),
                        ('bool_3', 'yeah', 'nah', 'N/A'),
                        ('bool_4', 'yup', 'nope', False),
                        ('bool_5', 'yeah', 'nah', 'N/A'),
                        ('bool_6', 'yup', 'nope', False)]
        expected = {'bool_1': True, 'bool_2': False,
                    'bool_3': False, 'bool_4': True,
                    'bool_5': 'N/A', 'bool_6': False}
        cb = dataflow_utils.ConvertBooleans(bool_changes, include_defaults = False)
        self.assertEqual(next(cb.process(datum)), expected)

    def test_format_and_classify_address(self):
        datum = [{'pii_street_num': '', 'street': None, 'cross_street': '', 'city': 'Pittsburgh',
                  'pii_lat': 40.4366963, 'pii_long': -79.944755399999991},
                 {'pii_street_num': '', 'street': None, 'cross_street': '', 'city': 'Pittsburgh',
                  'pii_lat': 40.4283632, 'pii_long': -79.973572699999991},
                 {'pii_street_num': '', 'street': None, 'cross_street': '', 'city': 'Pittsburgh',
                  'pii_lat': 40.4366963, 'pii_long': -79.944755399999991},
                 {'pii_street_num': '', 'street': None, 'cross_street': '', 'city': 'Pittsburgh',
                  'pii_lat': 40.4418296, 'pii_long': -80.0003875},
                 {'pii_street_num': '', 'street': None, 'cross_street': '', 'city': 'Pittsburgh',
                  'pii_lat': 40.4331995, 'pii_long': -80.0171609}]

        loc_names = {
                "street_num_field"  : "pii_street_num",
                "street_name_field" : "street",
                "cross_street_field": "cross_street",
                "city_field"        : "city",
                "lat_field"         : "pii_lat",
                "long_field"        : "pii_long"
        }
        contains_pii = True

        expected = [{'pii_street_num': '', 'street': None, 'cross_street': '', 'city': 'Pittsburgh',
                     'pii_lat': '40.4366963', 'pii_long': '-79.94475539999999', 'pii_input_address': None,
                     'address_type': 'Coordinates Only'},
                    {'pii_street_num': '', 'street': None, 'cross_street': '', 'city': 'Pittsburgh',
                     'pii_lat': '40.4283632', 'pii_long': '-79.97357269999999', 'pii_input_address': None,
                     'address_type': 'Coordinates Only'},
                    {'pii_street_num': '', 'street': None, 'cross_street': '', 'city': 'Pittsburgh',
                     'pii_lat': '40.4366963', 'pii_long': '-79.94475539999999', 'pii_input_address': None,
                     'address_type': 'Coordinates Only'},
                    {'pii_street_num': '', 'street': None, 'cross_street': '', 'city': 'Pittsburgh',
                     'pii_lat': '40.4418296', 'pii_long': '-80.0003875', 'pii_input_address': None,
                     'address_type': 'Coordinates Only'},
                    {'pii_street_num': '', 'street': None, 'cross_street': '', 'city': 'Pittsburgh',
                     'pii_lat': '40.4331995', 'pii_long': '-80.0171609', 'pii_input_address': None,
                     'address_type': 'Coordinates Only'}]

        fca = dataflow_utils.FormatAndClassifyAddress(loc_field_names=loc_names, contains_pii=contains_pii)
        results = []
        for val in datum:
            result = next(fca.process(val))
            results.append(result)
        self.assertEqual(results, expected)

        loc_field_names = {
            "address_field": "pii_input_address",
            "lat_field": "pii_lat",
            "long_field": "pii_long"
        }
        del_org_input = False
        gmap_key = os.environ["GMAP_API_KEY"]

        expected_2 = [{'pii_street_num': '', 'street': None, 'cross_street': '', 'city': 'Pittsburgh',
                       'pii_input_address': None, 'address_type': 'Coordinates Only',
                       'pii_google_formatted_address': None,
                       'input_pii_lat': '40.4366963', 'input_pii_long': '-79.94475539999999',
                       'google_pii_lat': None, 'google_pii_long': None},
                      {'pii_street_num': '', 'street': None, 'cross_street': '', 'city': 'Pittsburgh',
                       'pii_input_address': None, 'address_type': 'Coordinates Only',
                       'pii_google_formatted_address': None,
                       'input_pii_lat': '40.4283632', 'input_pii_long': '-79.97357269999999',
                       'google_pii_lat': None, 'google_pii_long': None},
                      {'pii_street_num': '', 'street': None, 'cross_street': '', 'city': 'Pittsburgh',
                       'pii_input_address': None, 'address_type': 'Coordinates Only',
                       'pii_google_formatted_address': None,
                       'input_pii_lat': '40.4366963', 'input_pii_long': '-79.94475539999999',
                       'google_pii_lat': None, 'google_pii_long': None},
                      {'pii_street_num': '', 'street': None, 'cross_street': '', 'city': 'Pittsburgh',
                       'pii_input_address': None, 'address_type': 'Coordinates Only',
                       'pii_google_formatted_address': None,
                       'input_pii_lat': '40.4418296', 'input_pii_long': '-80.0003875',
                       'google_pii_lat': None, 'google_pii_long': None},
                      {'pii_street_num': '', 'street': None, 'cross_street': '', 'city': 'Pittsburgh',
                       'pii_input_address': None, 'address_type': 'Coordinates Only',
                       'pii_google_formatted_address': None,
                       'input_pii_lat': '40.4331995', 'input_pii_long': '-80.0171609',
                       'google_pii_lat': None, 'google_pii_long': None}]

        gmg = dataflow_utils.GoogleMapsGeocodeAddress(key=gmap_key, loc_field_names=loc_field_names,
                                                      del_org_input=del_org_input)
        results_2 = []
        for val in expected:
            result = next(gmg.process(val))
            results_2.append(result)
        self.assertEqual(results_2, expected_2)

    def test_filter_outliers(self):
        datum = {"num_bridges": 446, "num_super_bowls": 6}
        outliers_conv = [("num_bridges", 1, 445), ("num_super_bowls", 6, 9999)]
        expected = {"num_bridges": None, "num_super_bowls": 6}
        fo = dataflow_utils.FilterOutliers(outliers_conv)
        self.assertEqual(next(fo.process(datum)), expected)

    def test_google_maps_classify_and_geocode(self):
        datum = [{'streetName': '5TH AVE', 'streetNum': '5939', 'crossStreetName': '', 'cityName': 'Pittsburgh', 'latitude': 0.0, 'longitude': 0.0},
                 {'streetName': '53483u9TH AVE', 'streetNum': '99999', 'crossStreetName': '', 'cityName': 'Pittsburgh', 'latitude': 0.0, 'longitude': 0.0},
                 {"streetName": "VINCETON ST", "streetNum": "4041", "crossStreetName": "Pheasant Way", "cityName": "Pittsburgh", "latitude": 40.4916844, "longitude": -80.0225664},
                 {"streetName": "STANTON AVE", "streetNum": "5821-5823", "crossStreetName": "ROBLEY WAY", "cityName": "Pittsburgh", "latitude": 40.4703142, "longitude": -79.9221585},
                 {"streetName": "S 22ND ST", "streetNum": "", "crossStreetName": "E CARSON ST", "cityName": "Pittsburgh", "latitude": 40.4284295, "longitude": -79.9746395},
                 {"streetName": "CAREY WAY", "streetNum": "2100 BLK", "crossStreetName": "", "cityName": "Pittsburgh", "latitude": 40.4280339, "longitude": -79.9762925},
                 {"streetName": "Idlewood Ave", "streetNum": "2860", "crossStreetName": "", "cityName": "Carnegie", "latitude": 40.418436, "longitude": -80.072954},
                 {"streetName": "CALIFORNIA AVE", "streetNum": "2428", "crossStreetName": "", "cityName": "Pittsburgh", "latitude": 40.464607, "longitude": -80.032372},
                 {'streetNum': '', 'streetName': None, 'crossStreetName': '', 'cityName': 'Pittsburgh', 'latitude': 40.484164, 'longitude': -79.9259162},
                 {'streetNum': '', 'streetName': None, 'crossStreetName': '', 'cityName': 'Pittsburgh', 'latitude': 0.0, 'longitude': 0.0}]
        loc_field_names = {'address_field': '',
                           'street_num_field': 'streetNum',
                           'street_name_field': 'streetName',
                           'cross_street_field': 'crossStreetName',
                           'city_field': 'cityName',
                           'lat_field': 'latitude',
                           'long_field': 'longitude'}
        expected = [{"pii_google_formatted_address": "5939 Fifth Ave, Pittsburgh, PA 15232, USA", "pii_input_address": "5939 5TH AVE, Pittsburgh", 'streetName': '5TH AVE', 'streetNum': '5939', 'crossStreetName': '', 'cityName': 'Pittsburgh', 'latitude': '40.4519661', 'longitude': '-79.924539', 'address_type': 'Precise'},
                    {"pii_google_formatted_address": None, "pii_input_address": "99999 53483u9TH AVE, Pittsburgh", "streetName": "53483u9TH AVE", "streetNum": "99999", "crossStreetName": "", 'cityName': "Pittsburgh", "latitude": None, "longitude": None, "address_type": "Unmappable"},
                    {"pii_google_formatted_address": "4041 Vinceton St, Pittsburgh, PA 15214, USA", "pii_input_address": "4041 VINCETON ST, Pittsburgh", "streetName": "VINCETON ST", "streetNum": "4041", "crossStreetName": "Pheasant Way", "cityName": "Pittsburgh", 'latitude': '40.4916844', 'longitude': '-80.0225664', 'address_type': 'Precise'},
                    {"pii_google_formatted_address": "5821 Stanton Ave, Pittsburgh, PA 15206, USA", "pii_input_address": "5821-5823 STANTON AVE, Pittsburgh", "streetName": "STANTON AVE", "streetNum": "5821-5823", "crossStreetName": "ROBLEY WAY", "cityName": "Pittsburgh", 'latitude': '40.4703142', 'longitude': '-79.9221585', 'address_type': 'Underspecified'},
                    {"pii_google_formatted_address": "S 22nd St & E Carson St, Pittsburgh, PA 15203, USA", "pii_input_address": "S 22ND ST and E CARSON ST, Pittsburgh", "streetName": "S 22ND ST", "streetNum": "", "crossStreetName": "E CARSON ST", "cityName": "Pittsburgh", 'latitude': '40.4284295', 'longitude': '-79.9746395', 'address_type': 'Intersection'},
                    {"pii_google_formatted_address": "2100 Carey Way, Pittsburgh, PA 15203, USA", "pii_input_address": "2100 BLK CAREY WAY, Pittsburgh", "streetName": "CAREY WAY", "streetNum": "2100 BLK", "crossStreetName": "", "cityName": "Pittsburgh", 'latitude': '40.4280339', 'longitude': '-79.9762925', 'address_type': 'Underspecified'},
                    {"pii_google_formatted_address": "2860 Idlewood Ave, Carnegie, PA 15106, USA", "pii_input_address": "2860 Idlewood Ave, Carnegie", "streetName": "Idlewood Ave", "streetNum": "2860", "crossStreetName": "", "cityName": "Carnegie", 'latitude': '40.4184411', 'longitude': '-80.07296219999999', 'address_type': 'Precise'},
                    {"pii_google_formatted_address": "2428 California Ave, Pittsburgh, PA 15212, USA", "pii_input_address": "2428 CALIFORNIA AVE, Pittsburgh", "streetName": "CALIFORNIA AVE", "streetNum": "2428", "crossStreetName": "", "cityName": "Pittsburgh", 'latitude': '40.4645768', 'longitude': '-80.0323918', 'address_type': 'Precise'},
                    {'pii_google_formatted_address': None, "pii_input_address": None, 'streetNum': '', 'streetName': None, 'crossStreetName': '', 'cityName': 'Pittsburgh', 'latitude': '40.484164', 'longitude': '-79.9259162', 'address_type': 'Coordinates Only'},
                    {'pii_google_formatted_address': None, "pii_input_address": None, 'streetNum': '', 'streetName': None, 'crossStreetName': '', 'cityName': 'Pittsburgh', 'latitude': 0.0, 'longitude': 0.0, 'address_type': 'Missing'}]
        gcg = dataflow_utils.GoogleMapsClassifyAndGeocode(key=os.environ['GMAP_API_KEY'],
                                                          loc_field_names=loc_field_names,
                                                          partitioned_address=True)
        results = []
        for val in datum:
            result = next(gcg.process(val))
            results.append(result)
        self.assertEqual(results, expected)
        datum_2 = [{'address': '414 Grant St, Pittsburgh, PA', 'lat': 40.0, 'long': -80.0},
                   {'address': '520 Chestnut St, Philadelphia, PA', 'lat': 39.0, 'long': -75.0},
                   {'address': '240 Hays Ave, Mt Oliver, PA 15210', 'lat': 40.0, 'long': -79.0}]
        loc_field_names_2 = {'address_field': 'address',
                             'lat_field': 'lat',
                             'long_field': 'long'}
        expected_2 = [{'address': '414 Grant St, Pittsburgh, PA', 'pii_input_address': '414 Grant St, Pittsburgh, PA', 'pii_google_formatted_address': '414 Grant St, Pittsburgh, PA 15219, USA', 'lat': '40.4382355', 'long': '-79.9966742', 'address_type': 'Precise'},
                      {'address': '520 Chestnut St, Philadelphia, PA', 'pii_input_address': '520 Chestnut St, Philadelphia, PA', 'pii_google_formatted_address': 'Independence Hall, 520 Chestnut St, Philadelphia, PA 19106, USA', 'lat': '39.9488737', 'long': '-75.1500233', 'address_type': 'Precise'},
                      {'address': '240 Hays Ave, Mt Oliver, PA 15210', 'pii_input_address': '240 Hays Ave, Mt Oliver, PA 15210', 'pii_google_formatted_address': '240 Hays Ave, Mount Oliver, PA 15210, USA', 'lat': '40.4141454', 'long': '-79.9875431', 'address_type': 'Precise'}]
        gcg_2 = dataflow_utils.GoogleMapsClassifyAndGeocode(key=os.environ['GMAP_API_KEY'],
                                                            loc_field_names=loc_field_names_2,
                                                            partitioned_address=False)
        results_2 = []
        for val in datum_2:
            result = next(gcg_2.process(val))
            results_2.append(result)
        self.assertEqual(results_2, expected_2)

    def test_geocode_address(self):
        datum = [{'ADDRESS': '5939 5TH AVE, Pittsburgh, PA 15232'}, {'ADDRESS': '9999 500TH AVE, PA'}]
        address_field = 'ADDRESS'
        expected = [{'ADDRESS': '5939 5TH AVE, Pittsburgh, PA 15232', 'lat': 40.45197335724138, 'long': -79.924606186473},
                    {'ADDRESS': '9999 500TH AVE, PA', 'lat': None, 'long': None}]
        gca = dataflow_utils.GeocodeAddress(address_field)
        results = []
        for val in datum:
            result = next(gca.process(val))
            results.append(result)
        self.assertEqual(results, expected)

    def test_swap_field_names(self):
        datum = {'exampleColumn': 'foo', 'anotherExample': 'bar'}
        name_changes = [('exampleColumn', 'newExampleColumn'), ('anotherExample', 'newAnotherExample')]
        expected = {'newExampleColumn': 'foo', 'newAnotherExample': 'bar'}
        sfn = dataflow_utils.SwapFieldNames(name_changes)
        self.assertEqual(next(sfn.process(datum)), expected)


    def test_get_date_strings_from_unix(self):
        datum = {'unix_timestamp': 1602619169}
        date_column_names = [('unix_timestamp', 'string_timestamp_utc', 'string_timestamp_east')]
        expected = {'unix_timestamp': 1602619169, 'string_timestamp_utc': '2020-10-13 19:59:29+00:00', 'string_timestamp_east': '2020-10-13 15:59:29-04:00'}
        gds = dataflow_utils.GetDateStringsFromUnix(date_column_names)
        self.assertEqual(next(gds.process(datum)), expected)

    def test_filter_fields(self):
        datum = {'city': 'pittsburgh', 'state': 'pa'}
        relevant_fields = 'state'
        expected = {'city': 'pittsburgh'}
        ff = dataflow_utils.FilterFields(relevant_fields)
        self.assertEqual(next(ff.process(datum)), expected)

    def test_filter_fields_exclude(self):
        datum = {'city': 'pittsburgh', 'state': 'pa'}
        relevant_fields = 'state'
        expected = {'state': 'pa'}
        ff = dataflow_utils.FilterFields(relevant_fields, exclude_relevant_fields=False)
        self.assertEqual(next(ff.process(datum)), expected)

    def test_replace_pii(self):
        datum = [{'comments': 'remove pothole'},
                 {'comments': 'John Doe is causing a lot of noise'},
                 {'comments': 'plow snow on Smith St and on 1st and Murray, notify me at jdoe@email.com when done'},
                 {'comments': ''},
                 {'comments': 'I saw Ms. Smith littering'},
                 {'comments': 'Timmy Smith woke up the whole neighborhood by listening to The Smiths too loud. Call him at 412-111-2222 to make him stop'}]
        input_field = 'comments'
        new_field_name = 'anon_comments'
        retain_location = True
        info_types = [{"name": "PERSON_NAME"}, {"name": "EMAIL_ADDRESS"}, {"name": "PHONE_NUMBER"}]
        expected = [{'comments': 'remove pothole', 'anon_comments': 'remove pothole'},
                    {'comments': 'John Doe is causing a lot of noise',
                     'anon_comments': '[PERSON_NAME] is causing a lot of noise'},
                    {'comments': 'plow snow on Smith St and on 1st and Murray, notify me at jdoe@email.com when done',
                     'anon_comments': 'plow snow on Smith_St and on 1st and_Murray, notify me at [EMAIL_ADDRESS] when done'},
                    {'comments': '',
                     'anon_comments': 'No comment'},
                    {'comments': 'I saw Ms. Smith littering',
                     'anon_comments': 'I saw [PERSON_NAME] littering'},
                    {'comments': 'Timmy Smith woke up the whole neighborhood by listening to The Smiths too loud. Call him at 412-111-2222 to make him stop',
                     'anon_comments': '[PERSON_NAME] woke up the whole neighborhood by listening to The Smiths too loud. Call him at [PHONE_NUMBER] to make him stop'}]

        rp = dataflow_utils.ReplacePII(input_field, new_field_name, retain_location, info_types, 'data-rivers-testing', 'user_defined_data')
        results = []
        for val in datum:
            result = next(rp.process(val))
            results.append(result)
        self.assertEqual(results, expected)

    def test_standardize_times(self):
        """
        Author : Jason Ficorilli
        Date   : June 2021
        This test function confirms that the StandardizeTimes utility function will successfully strip the
        timezone from a timestring input and instead localize the times using the timezone name
        provided by the developer.
        This test case was developed with the purpose of verifying that StandardizeTimes would still work if
        provided with conflicting information. The two timestrings provided in the datum dictionary include
        timezone definitions (UTC, -04:00) that contradict the timezone names that are passed into the
        StandardizeTimes utility function (America/Denver, UTC).
        """
        datum = {'openedDate': 'Fri July 19 03:21:55 UTC 2019', 'closedDate': '2021-05-01 01:44:00-04:00'}
        params = [('openedDate', 'America/Denver'), ('closedDate', 'UTC')]
        expected = datum.copy()
        expected.update(dict(openedDate_UTC='2019-07-19 09:21:55+00:00',
                             openedDate_EAST='2019-07-19 05:21:55-04:00',
                             openedDate_UNIX=1563528115.0,
                             closedDate_UTC='2021-05-01 01:44:00+00:00',
                             closedDate_EAST='2021-04-30 21:44:00-04:00',
                             closedDate_UNIX=1619833440.0))
        tst = dataflow_utils.StandardizeTimes(params)
        self.assertEqual(next(tst.process(datum)), expected)

    def test_standardize_times_with_api(self):
        """
        Author : Pranav Banthia / Jason Ficorilli
        Date   : June 2021
        For this test function, we use the current date which is datetime.now() as the starting point and then subtract
        a random timedelta from it to have multiple combinations of date/hour/minute values.
        Every timestamp is formatted in a different timezone/datetime format to ensure robustness of our dataflow util
        function. Since we have randomly generated timestamps, we use a third part API to create our expected test string.
        """
        # Replacing the second and microsecond ensures that the unix timestamp is not a floating point number
        datetime_ = datetime.datetime.now().replace(second=0, microsecond=0)

        # Pre-defining a list of different datetime formats
        dt_formats = ["%Y-%m-%dT%H:%M:%S", "%Y/%m/%d %H:%M:%S %z", "%Y.%m.%d %H:%M:%S", "%a %B %d %H:%M:%S %Y", "%c",
                      "%d %b, %Y %H:%M:%S", "%m/%d/%Y, %H:%M:%S", "%b %d %Y %H:%M:%S", "%A %d-%B-%Y %H:%M:%S"]

        # Third party API Endpoints
        date_to_unix_ = "https://showcase.api.linx.twenty57.net/UnixTime/tounix"
        unix_to_timezone_ = "https://showcase.api.linx.twenty57.net/UnixTime/fromunixtimestamp"
        try:
            for timezone in pytz.common_timezones:
                # We subtract a random timedelta between 1000 and 10000 minutes to ensure we have a different combination of hour/minute/day
                # and date for every iteration
                datetime_ -= datetime.timedelta(minutes=random.randint(low=1000, high=10000))

                # Finally we localize the datetime object to the test timezones which are defined above
                loc_time = pytz.timezone(timezone).localize(datetime_, is_dst=None)

                # Randomly choosing one of pre-defined datetime formats
                formatted_loc_time = loc_time.strftime(dt_formats[random.randint(low=0, high=len(dt_formats))])

                # API calls to get the unix timestamp and UTC time respectively for given datetime object
                api_request_unix = requests.get(url=date_to_unix_,
                                                params={'date': loc_time})
                api_request_utc = requests.post(url=unix_to_timezone_,
                                                json={"UnixTimeStamp": api_request_unix.json(), "Timezone": ""})

                # Formatting the output the way it would be returned from the dataflow_utils function
                utc_time = datetime.datetime.strptime(str(api_request_utc.json()['Datetime']), "%Y-%m-%dT%H:%M:%S%z")
                east_time = loc_time.astimezone(tz=pytz.timezone("America/New_York"))

                expected = dict(openedDate=str(formatted_loc_time),
                                openedDate_UNIX=float(api_request_unix.json()),
                                openedDate_UTC=str(utc_time),
                                openedDate_EAST=str(east_time))

                # Test our expected output against the values returned from dataflow utils standardize times
                datum = {'openedDate': formatted_loc_time}
                param = [('openedDate', timezone)]
                tst = dataflow_utils.StandardizeTimes(param)
                self.assertEqual(next(tst.process(datum)), expected)
        except pytz.NonExistentTimeError:
            print('Exception for timezone {} and time format {}'.format(timezone, formatted_loc_time))

    def test_reformat_phone_number(self):
        """
        Author : Pranav Banthia
        Date   : July 10 2021
        This function performs unit testing on the reformat phone number dataflow util helper. We first test it
        on US Phone numbers and then an international phone number
        """
        # US Phone Number
        datum = ["+1(412)-6368126", "+1-4126368126", "14126368126", "412-636-8126", "412,636,8126", "412.636/8126",
                 "412+636+8126", "$ 4 1 2 6 3 6 8 1 2 6 /"]
        expected = "+1 (412) 636-8126"
        rpn = dataflow_utils.ReformatPhoneNumbers()
        for number in datum:
            self.assertEqual(next(rpn.process(number)), expected)

        # International Phone Number
        datum = ["+44 7911 123456", "+44(791)-1123456", "+44-7911123456", "447911123456", "44-791-112-3456",
                 "44,791,112,3456", "44.791.112/3456", "+44+791+112+3456", "$ 4 4 7 9 1 1 1 2 3 4 5 6 /"]
        expected = "+44 (791) 112-3456"
        for number in datum:
            self.assertEqual(next(rpn.process(number)), expected)
            
    def test_lat_long_reformat(self):
        """
        Author : Pranav Banthia
        Date   : July 10 2021
        This function performs unit testing on the Latitude Longitude dataflow util helper.
        """
        datum = [(45.18492716, 130.8153100), (18.1738281, 100.46518390)]
        expected = [(45.185, 130.815), (18.174, 100.465)]
        llr = dataflow_utils.LatLongReformat()
        for i in range(len(datum)):
            self.assertTupleEqual(next(llr.process(datum[i])), expected[i])

    def test_anonymize_block_address(self):
        """
        Author : Pranav Banthia
        Date   : June 30 2021
        This function performs unit testing on the anonymize block number dataflow util helper. accuracies specify the
        different number of digits in the block num that we wished to mask every time
        """
        accuracies = [10, 100, 1000]
        datum = ["513 N. Neville St, Apt A1, Pittsburgh", "5565 Fifth Avenue, Apt D206, Pittsburgh"]
        expected = [
                     [("513", "N. Neville St", 510),
                      ("513", "N. Neville St", 500),
                      ("513", "N. Neville St", 0)],

                     [("5565", "Fifth Avenue", 5560),
                      ("5565", "Fifth Avenue", 5500),
                      ("5565", "Fifth Avenue", 5000)]
                   ]
        for i in range(len(datum)):
            for j in range(len(accuracies)):
                aab = dataflow_utils.AnonymizeAddressBlock(accuracy=accuracies[j])
                self.assertEqual(next(aab.process(datum[i])),expected[i][j])
      

if __name__ == '__main__':
    unittest.main()
