from __future__ import absolute_import
from __future__ import division
# patches unittest.TestCase to be python3 compatible
import os
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

    def test_build_dashburgh_street_tix_query(self):
        dataset = 'qalert'
        raw_table = 'backup_alr'
        new_table_name = 'dashburgh_street_tix'
        is_deduped = True
        id_field = 'group_id'
        group_field = 'request_type_name'
        limit = 10
        start_time = 1577854800
        field_groups = {group_field: [
                            'Angle Iron', 'Barricades', 'City Steps, Need Cleared',
                            'Curb/Request for Asphalt Windrow', 'Drainage/Leak',
                            'Graffiti, Removal', 'Leaves/Street Cleaning', 'Litter', 'Litter Can',
                            'Litter Can, Public', 'Litter, Public Property', 'Overgrowth',
                            'Port A Potty', 'Potholes', 'Public Right of Way', 'Salt Box',
                            'Snow/Ice removal', 'Street Cleaning/Sweeping', 'Trail Maintenance',
                            'Tree Fallen Across Road', 'Tree Fallen Across Sidewalk'],
                        'dept': ['DPW - Division 1', 'DPW - Division 2',
                                 'DPW - 2nd Division', 'DPW - Division 3',
                                 'DPW - Division 5', 'DPW - Division 6',
                                 'DPW - Street Maintenance']
                        }
        expected = f"""
        CREATE OR REPLACE TABLE `data-rivers-testing.qalert.dashburgh_street_tix` AS 
        SELECT DISTINCT group_id AS id, dept, tix.request_type_name, closed_date_est
        FROM `data-rivers-testing.qalert.backup_alr` tix
        INNER JOIN
            (SELECT request_type_name, COUNT(*) AS `count`
            FROM `data-rivers-testing.qalert.backup_alr`
            WHERE request_type_name  IN ('Angle Iron', 'Barricades', 'City Steps, Need Cleared', 
                'Curb/Request for Asphalt Windrow', 'Drainage/Leak', 
                'Graffiti, Removal', 'Leaves/Street Cleaning', 'Litter', 'Litter Can', 
                'Litter Can, Public', 'Litter, Public Property', 'Overgrowth', 
                'Port A Potty', 'Potholes', 'Public Right of Way', 
                'Salt Box', 'Snow/Ice removal', 'Street Cleaning/Sweeping',
                'Trail Maintenance', 'Tree Fallen Across Road', 'Tree Fallen Across Sidewalk')
            AND dept IN ('DPW - Division 1', 'DPW - Division 2', 'DPW - 2nd Division',
                        'DPW - Division 3', 'DPW - Division 5', 'DPW - Division 6', 
                        'DPW - Street Maintenance')
            GROUP BY request_type_name
            ORDER BY `count` DESC
            LIMIT 10) top_types
        ON tix.request_type_name = top_types.request_type_name
        WHERE tix.request_type_name IN ('Angle Iron', 'Barricades', 
            'City Steps, Need Cleared', 'Curb/Request for Asphalt Windrow', 
            'Drainage/Leak', 'Graffiti, Removal', 'Leaves/Street Cleaning', 
            'Litter', 'Litter Can', 'Litter Can, Public', 'Litter, Public Property', 
            'Overgrowth', 'Port A Potty', 'Potholes', 'Public Right of Way', 
            'Salt Box', 'Snow/Ice removal', 'Street Cleaning/Sweeping',
            'Trail Maintenance', 'Tree Fallen Across Road', 'Tree Fallen Across Sidewalk')
        AND dept IN ('DPW - Division 1', 'DPW - Division 2', 'DPW - 2nd Division',
                    'DPW - Division 3', 'DPW - Division 5', 'DPW - Division 6', 
                    'DPW - Street Maintenance')
        AND status_name = 'closed'
        AND create_date_unix >= 1577854800
        """
        output = af2_airflow_utils.build_dashburgh_street_tix_query(dataset, raw_table, new_table_name,
                                                                    is_deduped, id_field, group_field,
                                                                    limit, start_time, field_groups)
        self.assertEqual(re.sub('\s+', ' ', output), re.sub('\s+', ' ', expected))

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

    def test_gcs_to_email(self):
        bucket = f"pghpa_ceridian"
        file_path = "data_sharing/time_balance_mismatches.csv"
        recipients = [os.environ['EMAIL']]
        cc = [os.environ['OFFICE365_UN']]
        subject = "ALERT: Time Bank Mismatches Detected"
        message = "Attached is an extract of all time bank balances that differ between the Ceridian and InTime source systems."
        attachment_name = "time_balance_mismatches"
        on_day = (True, 2)
        af2_airflow_utils.gcs_to_email(bucket, file_path, recipients, cc, subject, message, attachment_name, on_day)

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
