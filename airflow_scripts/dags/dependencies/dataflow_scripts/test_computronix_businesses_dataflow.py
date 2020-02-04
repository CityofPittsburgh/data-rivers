from __future__ import absolute_import
from __future__ import division

import logging
import unittest

# patches unittest.TestCase to be python3 compatible
import future.tests.base  # pylint: disable=unused-import

from fastavro.validation import validate

from dataflow_utils import get_schema
from dataflow_test_utils import set_up
from computronix_businesses_dataflow import FormatColumnNames, ConvertTypes


class ComputronixTradesDataFlowTest(unittest.TestCase):
    set_up()
    RECORD = {
        "LICENSENUMBER": "17-SNC00002777",
        "LICENSETYPENAME": "Sign Maintenance Certification",
        "NAICSCODE": "541857",
        "BUSINESSNAME": "LAMAR ADVERTISING",
        "PARCELNUMBER": "0044C00325000900",
        "LICENSESTATE": "Expired",
        "INITIALISSUEDATE": "2017-11-09T09:59:51-05:00",
        "MOSTRECENTISSUEDATE": "2017-11-09T10:37:16-05:00",
        "EFFECTIVEDATE": "2017-11-09T10:37:16-05:00",
        "EXPIRATIONDATE": "2018-11-09T10:37:16-05:00",
        "NUMBEROFLARGESIGNS": 1,
        "NUMBEROFSMALLSIGNS": 0,
        "NUMBEROFSIGNSTOTAL": 0
    }
    SCHEMA = get_schema('businesses_computronix')
    # need to use next() to access dict value because dataflow steps yield generators
    formatted = next(FormatColumnNames.process(FormatColumnNames(), RECORD))
    type_converted = next(ConvertTypes.process(ConvertTypes(), formatted))

    def test_format_column_names(self):
        expected = {
            u"license_number": "17-SNC00002777",
            u"license_type_name": "Sign Maintenance Certification",
            u"naics_code": "541857",
            u"business_name": "LAMAR ADVERTISING",
            u"license_state": "Expired",
            u"initial_issue_date": "2017-11-09T09:59:51-05:00",
            u"most_recent_issue_date": "2017-11-09T10:37:16-05:00",
            u"effective_date": "2017-11-09T10:37:16-05:00",
            u"expiration_date": "2018-11-09T10:37:16-05:00",
            u"insurance_expiration_date": None,
            u"number_of_employees": None,
            u"number_of_signs_total": 0,
            u"number_of_small_signs": 0,
            u"number_of_large_signs": 1,
            u"total_number_of_spaces": None,
            u"number_of_nonleased_pub_spaces": None,
            u"number_of_revgen_spaces": None,
            u"number_of_handicap_spaces": None,
            u"number_of_seats": None,
            u"number_of_nongambling_machines": None,
            u"number_of_pool_tables": None,
            u"number_of_jukeboxes": None,
            u"parcel_number": "0044C00325000900",
            u"address": None
        }
        self.assertEqual(sorted(expected), sorted(self.formatted))

    def test_convert_types(self):
        expected = {
            u"license_number": "17-SNC00002777",
            u"license_type_name": "Sign Maintenance Certification",
            u"naics_code": 541857,
            u"business_name": "LAMAR ADVERTISING",
            u"license_state": "Expired",
            u"initial_issue_date": "2017-11-09T09:59:51-05:00",
            u"most_recent_issue_date": "2017-11-09T10:37:16-05:00",
            u"effective_date": "2017-11-09T10:37:16-05:00",
            u"expiration_date": "2018-11-09T10:37:16-05:00",
            u"insurance_expiration_date": None,
            u"number_of_employees": None,
            u"number_of_signs_total": 0,
            u"number_of_small_signs": 0,
            u"number_of_large_signs": 1,
            u"total_number_of_spaces": None,
            u"number_of_nonleased_pub_spaces": None,
            u"number_of_revgen_spaces": None,
            u"number_of_handicap_spaces": None,
            u"number_of_seats": None,
            u"number_of_nongambling_machines": None,
            u"number_of_pool_tables": None,
            u"number_of_jukeboxes": None,
            u"parcel_number": "0044C00325000900",
            u"address": None
        }
        self.assertEqual(sorted(expected), sorted(self.type_converted))

    def test_schema(self):
        self.assertTrue(validate(self.type_converted, self.SCHEMA))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    unittest.main()
