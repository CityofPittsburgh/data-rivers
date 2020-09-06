from __future__ import absolute_import
from __future__ import division

import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import logging
import unittest

# patches unittest.TestCase to be python3 compatible
import future.tests.base  # pylint: disable=unused-import

from dags.dependencies.gcs_loaders import gcs_utils


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


if __name__ == '__main__':
    unittest.main()
