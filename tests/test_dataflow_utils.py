from __future__ import absolute_import
from __future__ import division
# patches unittest.TestCase to be python3 compatible
import future.tests.base  # pylint: disable=unused-import

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import unittest
import mock

from dags.dependencies.dataflow_scripts.dataflow_utils import dataflow_utils


class TestDataflowUtils(unittest.TestCase):

    def columns_camel_to_snake_case(self):
        datum = {'exampleColumn': 'foo', 'anotherExample': 'bar'}
        expected = {'example_column': 'foo', 'another_example': 'bar'}
        self.assertEqual(dataflow_utils.ColumnsCamelToSnakeCase.process(datum), expected)


if __name__ == '__main__':
    unittest.main()
