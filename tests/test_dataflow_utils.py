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
        datum_1 = {"ADDRESS": "5939 5TH AVE, Pittsburgh, PA 15232"}
        address_field_1 = "ADDRESS"
        expected_1 = {"ADDRESS": "5939 5TH AVE, Pittsburgh, PA 15232", 'lat': 40.4519734, 'long': -79.9246062}
        gca_1 = dataflow_utils.geocode_address(datum_1, address_field_1)
        gca_1['lat'] = round(gca_1['lat'], 7)
        gca_1['long'] = round(gca_1['long'], 7)
        self.assertEqual(gca_1, expected_1)

        datum_2 = {"ADDRESS": "9999 500TH AVE, PA 15"}
        expected_2 = {"ADDRESS": "9999 500TH AVE, PA 15", 'lat': None, 'long': None}
        gca_2 = dataflow_utils.geocode_address(datum_2, address_field_1)
        self.assertEqual(gca_2, expected_2)

        try:
            dataflow_utils.geocode_address(datum_1[address_field_1])
        except TypeError:
            pass

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
        Author : Pranav Banthia
        Date   : June 2021
        For this test function, we use the current date which is datetime.now() as the starting point and then subtract
        a random timedelta from it to have multiple combinations of date/hour/minute values.
        Every timestamp is formatted in a different timezone/datetime format to ensure robustness of our dataflow util
        function. Since we have randomly generated timestamps, we use a third part API to create our expected test string.
        """
        # replacing the second and microsecond ensures that the unix timestamp is not a floating point number
        datetime_ = datetime.datetime.now().replace(second=0, microsecond=0)

        # Pre-defining a list of different acceptable time zones and date time formats to ensure our function is capable
        # of handling any input format
        time_zones_ = ['America/Denver', 'America/New_York', 'UTC', 'Europe/London', 'America/New_York', 'GMT',
                       'America/Denver', 'Europe/London']
        dt_formats = ["%Y-%m-%dT%H:%M:%S", "%Y/%m/%d %H:%M:%S %z", "%Y.%m.%d %H:%M:%S", "%a %B %d %H:%M:%S %Z %Y",
                      "%a %B %d %H:%M:%S %Y", "%c", "%d %b, %Y %H:%M:%S", "%m/%d/%Y, %H:%M:%S"]

        # API Endpoints
        date_to_unix_ = "https://showcase.api.linx.twenty57.net/UnixTime/tounix"
        unix_to_timezone_ = "https://showcase.api.linx.twenty57.net/UnixTime/fromunixtimestamp"

        for i in range(0, 8):
            # We subtract a timedelta of 5000 minutes to ensure we have a different combination of hour/minute/day and
            # date for every iteration
            datetime_ -= datetime.timedelta(minutes=np.random.randint(low = 1000, high = 50000))

            # To find the UTC-EST offset we first localize the current time to eastern zone and then calculate the
            # offset. This is done only to ensures we take into account the daylight savings time
            _loc_time = pytz.timezone("America/New_York").localize(datetime_, is_dst=None)
            east_utc_offset = str(int(_loc_time.utcoffset().total_seconds() / 60 / 60))

            # Finally we localize the datetime object to the test timezones which are defined above
            loc_time = pytz.timezone(time_zones_[i]).localize(datetime_, is_dst=None)
            formatted_loc_time = loc_time.strftime(dt_formats[i])
            datum = {'openedDate': formatted_loc_time}
            param = [('openedDate', time_zones_[i])]

            # API calls to get the unix timestamp, eastern time and UTC time respectively for the given datetime object
            api_request_unix = requests.get(url=date_to_unix_,
                                            params={'date': loc_time})
            api_request_east = requests.post(url=unix_to_timezone_,
                                             json={"UnixTimeStamp": api_request_unix.json(), "Timezone": east_utc_offset})
            api_request_utc = requests.post(url=unix_to_timezone_,
                                            json={"UnixTimeStamp": api_request_unix.json(), "Timezone": ""})

            # Formatting the output the way it would be returned from the dataflow_utils function
            utc_time = datetime.datetime.strptime(str(api_request_utc.json()['Datetime']), "%Y-%m-%dT%H:%M:%S%z")
            east_time = datetime.datetime.strptime(str(api_request_east.json()['Datetime']), "%Y-%m-%dT%H:%M:%S%z")

            expected = dict(openedDate=str(formatted_loc_time),
                            openedDate_UNIX=float(api_request_unix.json()),
                            openedDate_UTC=str(utc_time),
                            openedDate_EAST=str(east_time))

            # Test our expected output against the values returned from dataflow utils standardize times
            tst = dataflow_utils.StandardizeTimes(param)
            self.assertEqual(next(tst.process(datum)), expected)

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
