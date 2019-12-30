from __future__ import absolute_import
from __future__ import division

import logging
import os
import unittest

# patches unittest.TestCase to be python3 compatible
import future.tests.base  # pylint: disable=unused-import

from fastavro.validation import validate

from .dataflow_test_utils import get_schema
from trash_cans_dataflow import ConvertToDicts

try:
    from avro.schema import Parse  # avro-python3 library for python3
except ImportError:
    from avro.schema import parse as Parse  # avro library for python2


class TrashCansDataFlowTest(unittest.TestCase):

    RECORD = '1,74,"2017-09-14T13:24:40.35","2019-12-02T02:17:17.327","1st Division","122 E North Ave","Pittsburgh",' \
        '"Pennsylvania",15212,"Central Northside",1,1,22,"1-6"'

    SCHEMA = get_schema('smart_trash_cans.avsc')

    def set_up(self):
        # Reducing the size of thread pools. Without this test execution may fail in
        # environments with limited amount of resources.
        filebasedsource.MAX_NUM_THREADS_FOR_SIZE_ESTIMATION = 2


    def test_convert_to_dicts(self):
        converted = ConvertToDicts.process(ConvertToDicts(), self.RECORD)
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
        self.assertEqual(expected, converted)


    def test_schema(self):
        converted = ConvertToDicts.process(ConvertToDicts(), self.RECORD)
        self.assertTrue(validate(converted[0], self.SCHEMA))
        os.remove('./smart_trash_cans.avsc')


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    unittest.main()
