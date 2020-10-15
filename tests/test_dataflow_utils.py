from __future__ import absolute_import
from __future__ import division
# patches unittest.TestCase to be python3 compatible
import future.tests.base  # pylint: disable=unused-import

import os
import sys
import unittest

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dags.dependencies.dataflow_scripts.dataflow_utils import dataflow_utils


class TestDataflowUtils(unittest.TestCase):

    def test_columns_camel_to_snake_case(self):
        datum = {'exampleColumn': 'foo', 'anotherExample': 'bar'}
        expected = {'example_column': 'foo', 'another_example': 'bar'}
        ccsc = dataflow_utils.ColumnsCamelToSnakeCase()
        self.assertEqual(next(ccsc.process(datum)), expected)

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

    def test_normalize_address(self):
        datum = {'address': '123 fifth Ave pittsburgh pa'}
        address_key = 'address'
        expected = {'address': '123 fifth Ave pittsburgh pa', 'normalized_address': '123 FIFTH AVE PITTSBURGH PA'}
        na = dataflow_utils.NormalizeAddress(address_key)
        self.assertEqual(next(na.process(datum)), expected)


if __name__ == '__main__':
    unittest.main()
