from __future__ import absolute_import
from __future__ import division
# patches unittest.TestCase to be python3 compatible
import datetime

import future.tests.base  # pylint: disable=unused-import
import unittest
import numpy as np
import requests

from dateutil import parser
from dataflow_utils import dataflow_utils
import pytz


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

    def test_geocode_address(self):
        datum = [{'ADDRESS': '5939 5TH AVE, Pittsburgh, PA 15232'}, {'ADDRESS': '9999 500TH AVE, PA'}, {'ADDRESS': '3214 jo hammer scuaire, pittsburgh 15'}]
        address_field = 'ADDRESS'
        expected = [{'ADDRESS': '5939 Fifth Ave, Pittsburgh, PA 15232, USA', 'lat': 40.4519661, 'long': -79.924539},
                    {'ADDRESS': '9999 500TH AVE, PA', 'lat': None, 'long': None},
                    {'ADDRESS': '3214 Joe Hammer Square, Pittsburgh, PA 15213, USA', 'lat': 40.4349491, 'long': -79.9615974}]
        gca = dataflow_utils.GeocodeAddress(address_field)
        self.assertEqual(next(gca.process(datum)), expected)

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
