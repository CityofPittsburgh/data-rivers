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
        CREATE OR REPLACE TABLE  `data-rivers-testing.weather.pittsburgh_vs_london_weather_comp` AS
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

    def test_build_sync_staging_table_query(self):
        dataset = 'qalert'
        new_table = 'temp_curr_status_merge'
        upd_table = 'backup_alr'
        src_table = 'backup_atcs'
        is_deduped = True
        upd_id_field = 'group_id'
        join_id_field = 'id'
        field_groups = [{'req_types': ['request_type_name', 'request_type_id', 'origin']},
                        {'geos': ['pii_street_num', 'street', 'cross_street',
                                  'street_id', 'cross_street_id', 'city',
                                  'pii_input_address', 'pii_google_formatted_address',
                                  'anon_google_formatted_address', 'address_type',
                                  'neighborhood_name', 'council_district', 'ward',
                                  'police_zone', 'fire_zone', 'dpw_streets',  'dpw_enviro',
                                  'dpw_parks', 'input_pii_lat', 'input_pii_long',
                                  'google_pii_lat', 'google_pii_long', 'input_anon_lat',
                                  'input_anon_long', 'google_anon_lat', 'google_anon_long']
                         }
                        ]
        comp_fields = [{'req_types': ['request_type_name', 'origin']},
                        {'geos': ['address_type', 'neighborhood_name', 'council_district',
                                  'ward', 'police_zone', 'fire_zone', 'dpw_streets',
                                  'dpw_enviro', 'dpw_parks']
                         }
                        ]
        expected = """
        CREATE OR REPLACE TABLE `data-rivers-testing.qalert.temp_curr_status_merge` AS 
        SELECT DISTINCT group_id, req_types.request_type_name, req_types.request_type_id, req_types.origin,
        geos.pii_street_num, geos.street, geos.cross_street, geos.street_id, geos.cross_street_id,
        geos.city, geos.pii_input_address, geos.pii_google_formatted_address, 
        geos.anon_google_formatted_address,  geos.address_type, geos.neighborhood_name, geos.council_district, 
        geos.ward, geos.police_zone, geos.fire_zone, geos.dpw_streets, geos.dpw_enviro, geos.dpw_parks,
        geos.input_pii_lat, geos.input_pii_long, geos.google_pii_lat, geos.google_pii_long, 
        geos.input_anon_lat, geos.input_anon_long, geos.google_anon_lat, geos.google_anon_long
        FROM `data-rivers-testing.qalert.backup_alr` upd
        INNER JOIN
        (SELECT DISTINCT id, request_type_name, request_type_id, origin
        FROM `data-rivers-testing.qalert.backup_atcs`) req_types
        ON upd.group_id = req_types.id
        INNER JOIN
        (SELECT DISTINCT id, pii_street_num, street, cross_street, street_id, cross_street_id,
                city, pii_input_address, pii_google_formatted_address, anon_google_formatted_address,
                address_type, neighborhood_name, council_district, ward, police_zone, fire_zone,
                dpw_streets, dpw_enviro, dpw_parks, input_pii_lat, input_pii_long, google_pii_lat, google_pii_long, 
                input_anon_lat, input_anon_long, google_anon_lat, google_anon_long
        FROM `data-rivers-testing.qalert.backup_atcs`) geos
        ON upd.group_id = geos.id
        WHERE IFNULL(upd.request_type_name, "") != IFNULL(req_types.request_type_name, "")
        OR IFNULL(upd.origin, "") != IFNULL(req_types.origin, "")
        OR IFNULL(upd.address_type, "") != IFNULL(geos.address_type, "")
        OR IFNULL(upd.neighborhood_name, "") != IFNULL(geos.neighborhood_name, "")
        OR IFNULL(upd.council_district, "") != IFNULL(geos.council_district, "")
        OR IFNULL(upd.ward, "") != IFNULL(geos.ward, "")
        OR IFNULL(upd.police_zone, "") != IFNULL(geos.police_zone, "")
        OR IFNULL(upd.fire_zone, "") != IFNULL(geos.fire_zone, "")
        OR IFNULL(upd.dpw_streets, "") != IFNULL(geos.dpw_streets, "")
        OR IFNULL(upd.dpw_enviro, "") != IFNULL(geos.dpw_enviro, "")
        OR IFNULL(upd.dpw_parks, "") != IFNULL(geos.dpw_parks, "")
        """
        output = af2_airflow_utils.build_sync_staging_table_query(dataset, new_table, upd_table,
                                                                  src_table, is_deduped, upd_id_field,
                                                                  join_id_field, field_groups, comp_fields)
        self.assertEqual(re.sub('\s+', ' ', output), re.sub('\s+', ' ', expected))

    def test_build_sync_update_query(self):
        dataset = 'qalert'
        upd_table = 'backup_alr'
        src_table = 'backup_atcs'
        id_field = 'group_id'
        upd_fields = ['pii_street_num', 'street', 'cross_street',
                      'street_id',  'cross_street_id', 'city',
                      'pii_input_address', 'pii_google_formatted_address']
        expected = """UPDATE `data-rivers-testing.qalert.backup_alr` upd SET
        upd.pii_street_num = temp.pii_street_num, upd.street = temp.street, 
        upd.cross_street = temp.cross_street, upd.street_id = temp.street_id, 
        upd.cross_street_id = temp.cross_street_id, upd.city = temp.city, 
        upd.pii_input_address = temp.pii_input_address, 
        upd.pii_google_formatted_address = temp.pii_google_formatted_address
        FROM `data-rivers-testing.qalert.backup_atcs` temp
        WHERE upd.group_id = temp.group_id
        """
        output = af2_airflow_utils.build_sync_update_query(dataset, upd_table, src_table,
                                                          id_field, upd_fields)
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
