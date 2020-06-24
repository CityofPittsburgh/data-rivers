from __future__ import absolute_import
from __future__ import division

import logging
import os
import unittest

# patches unittest.TestCase to be python3 compatible
import future.tests.base  # pylint: disable=unused-import

from fastavro.validation import validate

from dataflow_utils.dataflow_utils import get_schema
from dataflow_utils.dataflow_test_utils import set_up
from computronix_contractors_dataflow import FormatColumnNames, ConvertTypes


class ComputronixTradesDataFlowTest(unittest.TestCase):
    set_up()
    RECORD = {
        "LICENSENUMBER": "BL008027",
        "LICENSETYPENAME": "General Contractor",
        "NAICSCODE": "236227",
        "BUSINESSNAME": "ENGINE 30 ARCHITECTURE, LLC",
        "LICENSESTATE": "Expired",
        "INITIALISSUEDATE": "2017-11-09T09:10:47-05:00",
        "MOSTRECENTISSUEDATE": "2017-11-09T09:12:14-05:00",
        "EFFECTIVEDATE": "2017-11-09T09:12:14-05:00",
        "EXPIRATIONDATE": "2018-11-09T09:12:14-05:00"
    }
    SCHEMA = get_schema('contractors_computronix')
    # need to use next() to access dict value because dataflow steps yield generators
    formatted = next(FormatColumnNames.process(FormatColumnNames(), RECORD))
    type_converted = next(ConvertTypes.process(ConvertTypes(), formatted))

    def test_format_column_names(self):
        expected = {
            "license_number": "BL008027",
            "license_type_name": "General Contractor",
            "naics_code": "236227",
            "business_name": "ENGINE 30 ARCHITECTURE, LLC",
            "license_state": "Expired",
            "initial_issue_date": "2017-11-09T09:10:47-05:00",
            "most_recent_issue_date": "2017-11-09T09:12:14-05:00",
            "effective_date": "2017-11-09T09:12:14-05:00",
            "expiration_date": "2018-11-09T09:12:14-05:00"
        }
        self.assertEqual(sorted(expected), sorted(self.formatted))

    def test_convert_types(self):
        expected = {
            "license_number": "BL008027",
            "license_type_name": "General Contractor",
            "naics_code": 236227,
            "business_name": "ENGINE 30 ARCHITECTURE, LLC",
            "license_state": "Expired",
            "initial_issue_date": "2017-11-09T09:10:47-05:00",
            "most_recent_issue_date": "2017-11-09T09:12:14-05:00",
            "effective_date": "2017-11-09T09:12:14-05:00",
            "expiration_date": "2018-11-09T09:12:14-05:00"
        }
        self.assertEqual(sorted(expected), sorted(self.type_converted))


    def test_schema(self):
        self.assertTrue(validate(self.type_converted, self.SCHEMA))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    unittest.main()
