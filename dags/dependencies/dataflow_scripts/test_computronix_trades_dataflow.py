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
from computronix_trades_dataflow import FormatColumnNames, ConvertTypes


class ComputronixTradesDataFlowTest(unittest.TestCase):
    set_up()
    RECORD = {
        "LICENSENUMBER":"EL09927",
        "LICENSETYPENAME":"Electrical Trade",
        "NAICSCODE":"236217",
        "BUSINESSNAME":"Michael Conroy",
        "LICENSESTATE":"Active",
        "INITIALISSUEDATE":"2017-11-15T15:04:07-05:00",
        "MOSTRECENTISSUEDATE":"2019-09-13T00:00:00-04:00",
        "EFFECTIVEDATE":"2019-10-25T08:23:48-04:00",
        "EXPIRATIONDATE":"2020-10-24T00:00:00-04:00"
    }
    SCHEMA = get_schema('trade_licenses_computronix')
    # need to use next() to access dict value because dataflow steps yield generators
    formatted = next(FormatColumnNames.process(FormatColumnNames(), RECORD))
    type_converted = next(ConvertTypes.process(ConvertTypes(), formatted))

    def test_format_column_names(self):
        expected = {
            "license_number": "EL09927",
            "license_type_name": "Electrical Trade",
            "naics_code": "236217",
            "business_name": "Michael Conroy",
            "license_state": "Active",
            "initial_issue_date": "2017-11-15T15:04:07-05:00",
            "most_recent_issue_date": "2019-09-13T00:00:00-04:00",
            "effective_date": "2019-10-25T08:23:48-04:00",
            "expiration_date": "2020-10-24T00:00:00-04:00"
        }
        self.assertEqual(sorted(expected), sorted(self.formatted))


    def test_convert_types(self):
        expected = {
            "license_number": "EL09927",
            "license_type_name": "Electrical Trade",
            "naics_code": 236217,
            "business_name": "Michael Conroy",
            "license_state": "Active",
            "initial_issue_date": "2017-11-15T15:04:07-05:00",
            "most_recent_issue_date": "2019-09-13T00:00:00-04:00",
            "effective_date": "2019-10-25T08:23:48-04:00",
            "expiration_date": "2020-10-24T00:00:00-04:00"
        }
        self.assertEqual(sorted(expected), sorted(self.type_converted))

    def test_schema(self):
        self.assertTrue(validate(self.type_converted, self.SCHEMA))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    unittest.main()
