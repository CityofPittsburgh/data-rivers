from __future__ import absolute_import
from __future__ import division

import os
import sys
import argparse
import logging
import unittest
# patches unittest.TestCase to be python3 compatible
import future.tests.base  # pylint: disable=unused-import'

# allow module imports no matter which dir tests are run from
file_dir = os.path.dirname(__file__)
sys.path.append(file_dir)

from wprdc_ems_fire_gcs import execution_date_to_prev_quarter, filter_call_keys, RELEVANT_KEYS


class EmsFireTest(unittest.TestCase):

    def test_filter_call_keys(self):
        api_record = [{
            '_geom': None,
            'call_year': 2020,
            'service': 'EMS',
            'call_quarter': 'Q2',
            'priority_desc': 'EMS Advanced Life Support life threatening response with Advanced Life Support backup',
            'census_block_group_center__x': -80.0205885244536,
            'priority': 'E0',
            'census_block_group_center__y': 40.3951064319195,
            'call_id_hash': '0008A9DF0F86E146DD7EE05B204EEF',
            'city_name': 'PITTSBURGH',
            '_id': 1036335,
            'description_short': 'Removed',
            'geoid': '420031919002009',
            'city_code': 'PGH',
            '_the_geom_webmercator': None
        }]

        expected = {
            '_id': 1036335,
            'service': 'EMS',
            'call_quarter': 'Q2',
            'call_year': 2020,
            'description_short': 'Removed',
            'priority': 'E0',
            'priority_desc': 'EMS Advanced Life Support life threatening response with Advanced Life Support backup',
            'call_id_hash': '0008A9DF0F86E146DD7EE05B204EEF',
            'long': -80.0205885244536,
            'lat': 40.3951064319195,
        }

        self.assertEqual(expected, filter_call_keys(api_record))

    def test_execution_date_to_prev_quarter(self):
        dag_date = '2020-07-27'
        expected = 'Q2', 2020
        self.assertEqual(expected, execution_date_to_prev_quarter(dag_date))

    def test_execution_date_to_quarter(self):
        dag_date = '2020-07-27'
        exected = 'Q3', 2020
        self.assertEqual(expected, execution_date_to_quarter(dag_date))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    unittest.main()
