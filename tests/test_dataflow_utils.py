from __future__ import absolute_import
from __future__ import division
# patches unittest.TestCase to be python3 compatible
import datetime

import future.tests.base  # pylint: disable=unused-import
import unittest
import numpy as np
import requests
import json

from dateutil import parser
from dataflow_utils import dataflow_utils
import pytz
import os

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
        datum = {'count': '1', 'zip': 15213, 'temp': 72, 'day': 31.1,
                 'bool1': 'TRUE', 'bool2': 1, 'nan_float': np.nan, 'nan_int': np.nan, 'nan_str': np.nan}
        type_changes = [("count", 'int'), ("zip", 'str'), ("temp", 'float'),
                        ("day", 'int'), ("bool1", 'bool'), ("bool2", 'bool'),
                        ("nan_float", 'float'), ("nan_int", 'int'), ("nan_str", 'str')]
        expected = {'count': 1, 'zip': '15213', 'temp': 72.0, 'day': 31,
                    'bool1': True, 'bool2': True, 'nan_float': None, 'nan_int': None, 'nan_str': None}
        cdt = dataflow_utils.ChangeDataTypes(type_changes)
        self.assertEqual(next(cdt.process(datum)), expected)

    def test_google_maps_classify_and_geocode(self):
        datum = [{'streetName': 'Grasshopper Ln', 'streetNum': '123', 'crossStreetName': '', 'cityName': 'Pittsburgh'},
                 {'streetName': '5TH AVE', 'streetNum': '5939', 'crossStreetName': '', 'cityName': 'Pittsburgh'},
                 {'streetName': '53483u9TH AVE', 'streetNum': '99999', 'crossStreetName': '', 'cityName': 'Pittsburgh'},
                 {"streetName": "VINCETON ST", "streetNum": "4041", "crossStreetName": "Pheasant Way", "cityName": "Pittsburgh", "latitude": 40.4916844, "longitude": -80.0225664},
                 {"streetName": "STANTON AVE", "streetNum": "5821-5823", "crossStreetName": "ROBLEY WAY", "cityName": "Pittsburgh", "latitude": 40.4703142, "longitude": -79.9221585},
                 {"streetName": "S 22ND ST", "streetNum": "", "crossStreetName": "E CARSON ST", "cityName": "Pittsburgh", "latitude": 40.4284295, "longitude": -79.9746395},
                 {"streetName": "CAREY WAY", "streetNum": "2100 BLK", "crossStreetName": "", "cityName": "Pittsburgh", "latitude": 40.4280339, "longitude": -79.9762925},
                 {"streetName": "Idlewood Ave", "streetNum": "2860", "crossStreetName": "", "cityName": "Carnegie", "latitude": 40.418436, "longitude": -80.072954},
                 {"streetName": "CALIFORNIA AVE", "streetNum": "2428", "crossStreetName": "", "cityName": "Pittsburgh", "latitude": 40.464607, "longitude": -80.032372},
                 {'streetNum': '', 'streetName': None, 'crossStreetName': '', 'cityName': 'Pittsburgh', 'latitude': None, 'longitude': None}]
        address_field = 'user_specified_address'
        street_num_field = 'streetNum'
        street_name_field = 'streetName'
        cross_street_field = 'crossStreetName'
        city_field = 'cityName'
        lat_field = 'latitude'
        long_field = 'longitude'
        expected = [{"google_formatted_address": "123 Grasshopper Ln, Greentown, PA 18426, USA", "user_specified_address": "123 Grasshopper Ln, Pittsburgh", 'streetName': 'Grasshopper Ln', 'streetNum': '123', 'crossStreetName': '', 'cityName': 'Pittsburgh', 'latitude': 41.3634857, 'longitude': -75.2567009, 'address_type': 'Outside of City'},
                    {"google_formatted_address": "5939 Fifth Ave, Pittsburgh, PA 15232, USA", "user_specified_address": "5939 5TH AVE, Pittsburgh", 'streetName': '5TH AVE', 'streetNum': '5939', 'crossStreetName': '', 'cityName': 'Pittsburgh', 'latitude': 40.4519661, 'longitude': -79.924539, 'address_type': 'Precise'},
                    {"google_formatted_address": None, "user_specified_address": "99999 53483u9TH AVE, Pittsburgh", "streetName": "53483u9TH AVE", "streetNum": "99999", "crossStreetName": "", 'cityName': "Pittsburgh", "latitude": None, "longitude": None, "address_type": "Unmappable"},
                    {"google_formatted_address": "4041 Vinceton St, Pittsburgh, PA 15214, USA", "user_specified_address": "4041 VINCETON ST, Pittsburgh", "streetName": "VINCETON ST", "streetNum": "4041", "crossStreetName": "Pheasant Way", "cityName": "Pittsburgh", 'latitude': 40.4916844, 'longitude': -80.0225664, 'address_type': 'Precise'},
                    {"google_formatted_address": "5821 Stanton Ave, Pittsburgh, PA 15206, USA", "user_specified_address": "5821-5823 STANTON AVE, Pittsburgh", "streetName": "STANTON AVE", "streetNum": "5821-5823", "crossStreetName": "ROBLEY WAY", "cityName": "Pittsburgh", 'latitude': 40.4703142, 'longitude': -79.9221585, 'address_type': 'Underspecified'},
                    {"google_formatted_address": "S 22nd St & E Carson St, Pittsburgh, PA 15203, USA", "user_specified_address": "S 22ND ST and E CARSON ST, Pittsburgh", "streetName": "S 22ND ST", "streetNum": "", "crossStreetName": "E CARSON ST", "cityName": "Pittsburgh", 'latitude': 40.4284295, 'longitude': -79.9746395, 'address_type': 'Intersection'},
                    {"google_formatted_address": "2100 Carey Way, Pittsburgh, PA 15203, USA", "user_specified_address": "2100 BLK CAREY WAY, Pittsburgh", "streetName": "CAREY WAY", "streetNum": "2100 BLK", "crossStreetName": "", "cityName": "Pittsburgh", 'latitude': 40.4280339, 'longitude': -79.9762925, 'address_type': 'Underspecified'},
                    {"google_formatted_address": "2860 Idlewood Ave, Carnegie, PA 15106, USA", "user_specified_address": "2860 Idlewood Ave, Carnegie", "streetName": "Idlewood Ave", "streetNum": "2860", "crossStreetName": "", "cityName": "Carnegie", 'latitude': 40.418436, 'longitude': -80.072954, 'address_type': 'Precise'},
                    {"google_formatted_address": "2428 California Ave, Pittsburgh, PA 15212, USA", "user_specified_address": "2428 CALIFORNIA AVE, Pittsburgh", "streetName": "CALIFORNIA AVE", "streetNum": "2428", "crossStreetName": "", "cityName": "Pittsburgh", 'latitude': 40.4645768, 'longitude': -80.0323918, 'address_type': 'Precise'},
                    {'google_formatted_address': None, "user_specified_address": None, 'streetNum': '', 'streetName': None, 'crossStreetName': '', 'cityName': 'Pittsburgh', 'latitude': None, 'longitude': None, 'address_type': 'Unmappable'}]
        gcg = dataflow_utils.GoogleMapsClassifyAndGeocode(address_field, street_num_field, street_name_field, cross_street_field, city_field, lat_field, long_field)
        results = []
        for val in datum:
            result = next(gcg.process(val))
            results.append(result)
        self.assertEqual(results, expected)

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

    def test_get_date_string(self):
        datum = {'unix_timestamp': 1602619169}
        date_column_names = [('unix_timestamp', 'string_timestamp')]
        expected = {'unix_timestamp': 1602619169, 'string_timestamp': '2020-10-13 15:59:29 EDT'}
        gds = dataflow_utils.GetDateStrings(date_column_names)
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

    def test_standardize_times(self):
        datum = {'openedDate': 'Fri July 19 03:21:55 UTC 2019', 'closedDate': '2021-05-01 01:44:00-04:00'}
        params = [('openedDate', 'Mountain'), ('closedDate', 'UTC')]
        expected = datum.copy()
        expected.update(dict(openedDate_UTC=parser.parse('2019-07-19 09:21:55+00:00'),
                             openedDate_EST=parser.parse('2019-07-19 05:21:55-04:00'),
                             openedDate_UNIX=1563528115.0,
                             closedDate_UTC=parser.parse('2021-05-01 01:44:00+00:00'),
                             closedDate_EST=parser.parse('2021-04-30 21:44:00-04:00'),
                             closedDate_UNIX=1619833440.0))
        tst = dataflow_utils.StandardizeTimes(params)
        self.assertEqual(next(tst.process(datum)), expected)

    def test_standardize_times_with_api(self):
        datetime_ = datetime.datetime.now().replace(second=0, microsecond=0)
        time_zones_ = ['Mountain', 'Eastern', 'Universal', 'EU', 'New York', 'GMT', 'MST', 'Britain']
        dt_formats = ["%Y-%m-%dT%H:%M:%S", "%Y/%m/%d %H:%M:%S %z", "%Y.%m.%d %H:%M:%S", "%a %B %d %H:%M:%S %Z %Y",
                      "%a %B %d %H:%M:%S %Y", "%c", "%d %b, %Y %H:%M:%S", "%m/%d/%Y, %H:%M:%S"]

        date_to_unix_ = "https://showcase.api.linx.twenty57.net/UnixTime/tounix"
        unix_to_timezone_ = "https://showcase.api.linx.twenty57.net/UnixTime/fromunixtimestamp"

        for i in range(0, 8):
            datetime_ -= datetime.timedelta(minutes=5000)
            conv_tz = dataflow_utils.TZ_CONVERTER[time_zones_[i]]
            loc_time = pytz.timezone(conv_tz).localize(datetime_, is_dst=None)
            formatted_loc_time = loc_time.strftime(dt_formats[i])
            datum = {'openedDate': formatted_loc_time}
            param = [('openedDate', time_zones_[i])]

            api_request_unix = requests.get(url=date_to_unix_,
                                            params={'date': loc_time})
            api_request_est = requests.post(url=unix_to_timezone_,
                                            json={"UnixTimeStamp": api_request_unix.json(), "Timezone": "-4"})
            api_request_utc = requests.post(url=unix_to_timezone_,
                                            json={"UnixTimeStamp": api_request_unix.json(), "Timezone": ""})

            utc_time = datetime.datetime.strptime(str(api_request_utc.json()['Datetime']), "%Y-%m-%dT%H:%M:%S%z")
            est_time = datetime.datetime.strptime(str(api_request_est.json()['Datetime']), "%Y-%m-%dT%H:%M:%S%z")

            expected = dict(openedDate=str(formatted_loc_time),
                            openedDate_UNIX=float(api_request_unix.json()),
                            openedDate_UTC=str(utc_time),
                            openedDate_EST=str(est_time))

            tst = dataflow_utils.StandardizeTimes(param)
            self.assertEqual(next(tst.process(datum)), expected)


if __name__ == '__main__':
    unittest.main()
