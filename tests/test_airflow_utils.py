from __future__ import absolute_import
from __future__ import division
# patches unittest.TestCase to be python3 compatible
import datetime
import re
import unittest

from dateutil import parser
from google.cloud import bigquery, storage
#from deprecated import airflow_utils
#from af2_dags.dependencies.airflow_utils import airflow_utils as af2_airflow_utils
import af2_dags.dependencies.airflow_utils as af2_airflow_utils

bq_client = bigquery.Client()
storage_client = storage.Client()

class TestAirflowUtils(unittest.TestCase):

    def test_build_percentage_table_query(self):
        dataset = 'weather'
        raw_table = 'daily_weather'
        new_table_name = 'pittsburgh_vs_london_weather_comp'
        is_deduped = False
        id_field = 'unix_date_time'
        pct_field = 'conditions'
        categories = ['Pittsburgh', 'London (On Average)']
        hardcoded_vals = [{pct_field: 'Clouds', 'percentage': 90.5},
                          {pct_field: 'Rain', 'percentage': 9.0},
                          {pct_field: 'Clear', 'percentage': 0.5}]
        expected = f"""
        CREATE OR REPLACE TABLE  `data-rivers-testing.{dataset}.{new_table_name}` AS
        SELECT conditions, 
               100*(conditions_count / total) AS percentage, 
               'Pittsburgh' AS type
        FROM (
              SELECT conditions, COUNT(DISTINCT(unix_date_time)) AS conditions_count, SUM(COUNT(*)) OVER() AS total
              FROM `data-rivers-testing.weather.daily_weather` 
              GROUP BY conditions
        )
        UNION ALL
        SELECT 'Clouds' AS conditions, 90.5 AS percentage, 'London (On Average)' AS type
        UNION ALL
        SELECT 'Rain' AS conditions, 9.0 AS percentage, 'London (On Average)' AS type
        UNION ALL
        SELECT 'Clear' AS conditions, 0.5 AS percentage, 'London (On Average)' AS type
        ORDER BY type, percentage DESC
        """
        output = af2_airflow_utils.build_percentage_table_query(dataset, raw_table, new_table_name,
                                                                is_deduped, id_field, pct_field,
                                                                categories, hardcoded_vals)
        self.assertEqual(re.sub('\s+',' ', output),  re.sub('\s+',' ', expected))


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
            output = af2_airflow_utils.find_backfill_date(val['bucket'], val['subfolder'])
            self.assertEqual(output, str(expected[index].date()))
            index += 1

    def test_within_city_limits(self):
        datum = [{'address': '414 Grant St, Pittsburgh, PA 15219', 'lat': 40.4382355, 'long': -79.9966742, 'address_type': 'Precise'},
                 {'address': '123 Grasshopper Ln, Greentown, PA 18426', 'lat': 41.3634857, 'long': -75.2567009, 'address_type': 'Underspecified'},
                 {'address': '240 Hays Ave, Mount Oliver, PA 15210', 'lat': 40.4141454, 'long': -79.9875431, 'address_type': 'Intersection'}]
        coord_fields = {'long_field': 'long', 'lat_field': 'lat'}
        expected = [{'address': '414 Grant St, Pittsburgh, PA 15219', 'lat': 40.4382355, 'long': -79.9966742, 'address_type': 'Precise'},
                    {'address': '123 Grasshopper Ln, Greentown, PA 18426', 'lat': 41.3634857, 'long': -75.2567009, 'address_type': 'Outside of City'},
                    {'address': '240 Hays Ave, Mount Oliver, PA 15210', 'lat': 40.4141454, 'long': -79.9875431, 'address_type': 'Outside of City'}]
        index = 0
        for val in datum:
            output = af2_airflow_utils.build_city_limits_query(val, coord_fields)
            self.assertEqual(output, expected[index])
            index += 1


if __name__ == '__main__':
    unittest.main()
