from __future__ import absolute_import
from __future__ import division
# patches unittest.TestCase to be python3 compatible
import future.tests.base  # pylint: disable=unused-import
import unittest
import numpy as np

from dataflow_utils import dataflow_utils


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


if __name__ == '__main__':
    unittest.main()