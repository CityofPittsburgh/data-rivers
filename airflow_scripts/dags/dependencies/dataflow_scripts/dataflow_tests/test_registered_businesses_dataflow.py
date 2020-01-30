# TODO: uncomment all this once we can upgrade to python3

# from __future__ import absolute_import
# from __future__ import division
#
# import logging
# import os
# import unittest
#
# # patches unittest.TestCase to be python3 compatible
# import future.tests.base  # pylint: disable=unused-import
#
# from fastavro.validation import validate
#
# from from .dataflow_test_utils import get_public_schema, set_up
# from ..registered_businesses_dataflow import ConvertToDicts, AddNormalizedAddress
#
#
# class RegisteredBusinessesDataFlowTest(unittest.TestCase):
#     set_up()
#     RECORD = '"001033373","A FROST INC ","FROST CO","N/A","CO","MA","PITTSBURGH","PA","15222","1999-11-10",' \
#              '"1985-01-01","717 LIBERTY AVE PITTSBURGH, PA 15222"'
#
#     converted = ConvertToDicts.process(ConvertToDicts(), RECORD)
#     # normalized = AddNormalizedAddress.process(AddNormalizedAddress(), converted[0])
#
#     def test_convert_to_dicts(self):
#         expected = {
#             'acct_no': '001033373',
#             'name': 'A FROST INC',
#             'trade_name': 'FROST CO',
#             'desc_of_business': 'N/A',
#             'business_type': 'CO',
#             'address_type': 'MA',
#             'city': 'PITTSBURGH',
#             'state': 'PA',
#             'zip': 15222,
#             'date_created': '1999-11-10',
#             'business_start_date_in_pgh': '1985-01-01',
#             'address_full': '717 LIBERTY AVE PITTSBURGH PA 15222'
#         }
#         self.assertEqual(expected, self.converted[0])
#
#     # TODO: add this once we update to python3 and can use scourgify's normalize_address_record function
#     # def test_normalized_address(self):
#     #     self.assertEqual(self.normalized[0]['normalized_address'], '717 LIBERTY AVE PITTSBURGH PA 15222')
#
#
#     def tear_down(self):
#         os.remove('../registered_businesses.avsc')
#
#
# if __name__ == '__main__':
#     logging.getLogger().setLevel(logging.INFO)
#     unittest.main()
