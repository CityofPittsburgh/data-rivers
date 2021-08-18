from __future__ import absolute_import
from __future__ import division
# patches unittest.TestCase to be python3 compatible
import datetime

import future.tests.base  # pylint: disable=unused-import
import unittest
import numpy as np
import requests

from dateutil import parser
from google.cloud import bigquery, storage
import airflow_utils

import os

bq_client = bigquery.Client()
storage_client = storage.Client()

class TestAirflowUtils(unittest.TestCase):

    def test_set_backfill_date(self):
        datum = ['pghpa_accela', 'backfill/planning/planning_2019.json']
        expected_date = 'Oct 21, 2020'
        expected = parser.parse(expected_date)
        output = airflow_utils.set_backfill_date(datum[0], datum[1])
        self.assertEqual(output, expected.date())

    def test_find_latest_file_date(self):
        datum = ['pghpa_computronix', 'domi_permits']
        expected_date = 'Nov 9, 2020'
        expected = parser.parse(expected_date)
        output = airflow_utils.find_latest_file_date(datum[0], datum[1])
        self.assertEqual(output, expected.date())

    def test_log_missing_file(self):
        bucket_name = "pghpa_test_logging"
        datum_true = ['Field_Listings.avsc', 'pghpa_test', bucket_name]
        datum_false = ['test_file.csv', 'pghpa_wprdc', bucket_name]
        self.assertEqual(airflow_utils.log_missing_file(datum_true[0], datum_true[1], datum_true[2]), True)
        self.assertEqual(airflow_utils.log_missing_file(datum_false[0], datum_false[1], datum_false[2]), False)

if __name__ == '__main__':
    unittest.main()
