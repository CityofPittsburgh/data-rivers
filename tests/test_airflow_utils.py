from __future__ import absolute_import
from __future__ import division
# patches unittest.TestCase to be python3 compatible
import datetime

import unittest

from dateutil import parser
from google.cloud import bigquery, storage
from deprecated import airflow_utils

bq_client = bigquery.Client()
storage_client = storage.Client()

class TestAirflowUtils(unittest.TestCase):

    def test_find_backfill_date(self):
        datum = [{'bucket': 'pghpa_test_police', 'subfolder': '30_day_blotter'},
                 {'bucket': 'pghpa_twilio', 'subfolder': 'service_desk'},
                 {'bucket': 'pghpa_test_accela', 'subfolder': 'permits'},
                 {'bucket': 'pghpa_computronix', 'subfolder': 'domi_permits'},
                 {'bucket': 'pghpa_test', 'subfolder': 'home'}]
        # expected values won't always be up-to-date - accurate as of 8/20/2021
        expected_dates = ['July 12, 2021', 'Aug 19, 2021', 'July 14, 2021', 'Nov 8, 2020', str(datetime.date.today() - datetime.timedelta(days=1))]
        expected = []
        for date in expected_dates:
            expected.append(parser.parse(date))
        index = 0
        for val in datum:
            output = airflow_utils.find_backfill_date(val['bucket'], val['subfolder'])
            self.assertEqual(output, str(expected[index].date()))
            index += 1

    def test_within_city_limitss(self):
        datum = [{'address': '414 Grant St, Pittsburgh, PA 15219', 'lat': 40.4382355, 'long': -79.9966742, 'address_type': 'Precise'},
                 {'address': '123 Grasshopper Ln, Greentown, PA 18426', 'lat': 41.3634857, 'long': -75.2567009, 'address_type': 'Underspecified'},
                 {'address': '240 Hays Ave, Mount Oliver, PA 15210', 'lat': 40.4141454, 'long': -79.9875431, 'address_type': 'Intersection'}]
        coord_fields = {'long_field': 'long', 'lat_field': 'lat'}
        expected = [{'address': '414 Grant St, Pittsburgh, PA 15219', 'lat': 40.4382355, 'long': -79.9966742, 'address_type': 'Precise'},
                    {'address': '123 Grasshopper Ln, Greentown, PA 18426', 'lat': 41.3634857, 'long': -75.2567009, 'address_type': 'Outside of City'},
                    {'address': '240 Hays Ave, Mount Oliver, PA 15210', 'lat': 40.4141454, 'long': -79.9875431, 'address_type': 'Outside of City'}]
        index = 0
        for val in datum:
            output = airflow_utils.build_city_limits_query(val, coord_fields)
            self.assertEqual(output, expected[index])
            index += 1


if __name__ == '__main__':
    unittest.main()
