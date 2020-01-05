from __future__ import absolute_import
from __future__ import division

import logging
import os
import unittest

# patches unittest.TestCase to be python3 compatible
import future.tests.base  # pylint: disable=unused-import

from fastavro.validation import validate

from .dataflow_test_utils import get_schema, set_up
from computronix_businesses_dataflow import ConvertToDicts, AddNormalizedAddress


class ComputronixBusinessesDataFlowTest(unittest.TestCase):

    RECORD = '"001033371","A FROST INC ","FROST CO","N/A","CO","MA","PITTSBURGH","PA","15222","1999-11-10"', \
             '"1985-01-01","717 LIBERTY AVE PITTSBURGH, PA 15222"'

    SCHEMA = get_schema('registered_businesses.avsc')

    converted = ConvertToDicts.process(ConvertToDicts(), RECORD)
    normalized = AddNormalizedAddress.process(AddNormalizedAddress(), converted)

    set_up()

    def test_convert_to_dicts(self):
        expected = [{
            'acct_no': 001033371,
            'name': 'A FROST INC',
            'trade_name': 'FROST CO',
            'desc_of_business': 'N/A',
            'business_type': 'CO',
            'address_type': 'MA',
            'city': 'PITTSBURGH',
            'state': 'PA',
            'zip': 15222,
            'date_created': '1999-11-10',
            'business_start_date_in_pgh': '1985-01-01',
            'address_full': '717 LIBERTY AVE PITTSBURGH, PA 15222'
        }]
        self.assertEqual(expected, self.converted)

    # this part won't work until we can update to python3 and use scourgify's normalize_address_record function
    def test_normalized_address(self):
        self.assertEqual(self.normalized['normalized_address'], '717 LIBERTY AVE PITTSBURGH PA 15222')


    def test_schema(self):
        self.assertTrue(validate(normalized[0], self.SCHEMA))
        os.remove('./registered_businesses.avsc')


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    unittest.main()
