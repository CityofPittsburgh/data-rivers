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

    def test_find_backfill_date(self):
        datum = [{'bucket': 'pghpa_test_police', 'dir': '30_day_blotter'},
                 {'bucket': 'pghpa_twilio', 'dir': 'service_desk'},
                 {'bucket': 'pghpa_test_accela', 'dir': 'permits'},
                 {'bucket': 'pghpa_computronix', 'dir': 'domi_permits'},
                 {'bucket': 'pghpa_test', 'dir': 'home'}]
        expected_dates = ['July 12, 2021', 'Aug 19, 2021', 'July 14, 2021', 'Nov 8, 2020', 'Aug 19, 2021']
        expected = []
        for date in expected_dates:
            expected.append(parser.parse(date))
        index = 0
        for val in datum:
            output = airflow_utils.find_backfill_date(val['bucket'], val['dir'])
            self.assertEqual(output, str(expected[index].date()))
            index += 1


if __name__ == '__main__':
    unittest.main()
