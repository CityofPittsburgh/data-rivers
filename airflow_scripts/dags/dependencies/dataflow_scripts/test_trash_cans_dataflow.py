from __future__ import absolute_import
from __future__ import division

import logging
import os
import unittest

# patches unittest.TestCase to be python3 compatible
import future.tests.base  # pylint: disable=unused-import

from fastavro.validation import validate

from dataflow_utils import get_schema
from dataflow_test_utils import set_up
from trash_cans_dataflow import ConvertToDicts


class TrashCansDataFlowTest(unittest.TestCase):
    set_up()
    RECORD = '1,74,"2017-09-14T13:24:40.35","2019-12-02T02:17:17.327","1st Division","122 E North Ave","Pittsburgh",' \
        '"Pennsylvania",15212,"Central Northside",1,1,22,"1-6"'
    SCHEMA = get_schema('smart_trash_cans')
    converted = ConvertToDicts.process(ConvertToDicts(), RECORD)

    def test_convert_to_dicts(self):
        expected = [{
            'container_id': 1,
            'receptacle_model_id': 74,
            'assignment_date': '2017-09-14T13:24:40.35',
            'last_updated_date': '2019-12-02T02:17:17.327',
            'group_name': '1st Division',
            'address': '122 E North Ave',
            'city': 'Pittsburgh',
            'state': 'Pennsylvania',
            'zip': 15212,
            'neighborhood': 'Central Northside',
            'dpw_division': 1,
            'council_district': 1,
            'ward': 22,
            'fire_zone': '1-6'
        }]
        self.assertEqual(expected, self.converted)

    def test_schema(self):
        self.assertTrue(validate(self.converted[0], self.SCHEMA))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    unittest.main()
