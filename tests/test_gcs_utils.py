from __future__ import absolute_import
from __future__ import division
# patches unittest.TestCase to be python3 compatible
import future.tests.base  # pylint: disable=unused-import

import os
import sys
import unittest

from dags.dependencies.gcs_loaders import gcs_utils


# TODO: mock mssql and oracle db connections, queries, results; mock google DLP

class TestGcsUtils(unittest.TestCase):

    def test_regex_filter(self):
        phone = '412-555-5555'
        phone1 = '412.555.5555'
        phone2 = '4125555555'
        phone3 = '+14125555555'
        email = 'james@pittsburghpa.gov'
        self.assertEqual('#########', gcs_utils.regex_filter(phone))
        self.assertEqual('#########', gcs_utils.regex_filter(phone1))
        self.assertEqual('#########', gcs_utils.regex_filter(phone2))
        self.assertEqual('+#########5', gcs_utils.regex_filter(phone3))
        self.assertEqual('####', gcs_utils.regex_filter(email))

    def test_time_to_seconds(self):
        self.assertEqual(gcs_utils.time_to_seconds('2020-09-06'), 1599350400)

    def test_filter_fields(self):
        result = [{'city': 'pittsburgh', 'state': 'pa'}, {'city': 'new york', 'state': 'ny'}]
        relevant_fields = ['state']
        expected = [{'state': 'pa'}, {'state': 'ny'}]
        self.assertEqual(gcs_utils.filter_fields(result, relevant_fields), expected)

    def test_execution_date_to_quarter(self):
        self.assertEqual(gcs_utils.execution_date_to_quarter('2020-09-06'), ('Q3', 2020))

    def test_execution_date_to_prev_quarter(self):
        self.assertEqual(gcs_utils.execution_date_to_prev_quarter('2020-09-06'), ('Q2', 2020))

    def test_json_linter(self):
        json = "{'id': 1}{'id': 2}{'id':3}"
        expected = "{'id': 1}\n{'id': 2}\n{'id':3}"
        self.assertEqual(gcs_utils.json_linter(json), expected)


if __name__ == '__main__':
    unittest.main()
