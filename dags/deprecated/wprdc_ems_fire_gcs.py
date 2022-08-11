import os
import argparse

from gcs_utils import json_to_gcs, get_wprdc_data, execution_date_to_prev_quarter, filter_fields

"""
This DAG runs monthly, with a query downstream to filter duplicates from the final table. Note that as with call date,
(obscured to quarter level, reported quarterly) location data is obscured to census block lat/long for privacy reasons. 
"""

parser = argparse.ArgumentParser()
parser.add_argument('-e', '--execution_date', dest='execution_date',
                    required=True, help='DAG execution date (YYYY-MM-DD)')
args = vars(parser.parse_args())

bucket = '{}_ems_fire'.format(os.environ['GCS_PREFIX'])

FIELDS_TO_REMOVE = [
    'city_name',
    'city_code',
    'geoid',
    '_the_geom_webmercator',
    '_geom'
]

quarter, year = execution_date_to_prev_quarter(args['execution_date'])

where_clauses = '"city_name" = \'PITTSBURGH\' AND "call_quarter" = \'{}\' ' \
                'AND "call_year" = \'{}\''.format(quarter, year)

ems_calls = get_wprdc_data(
    resource_id='ff33ca18-2e0c-4cb5-bdcd-60a5dc3c0418',
    where_clauses=where_clauses,
    fields_to_remove=FIELDS_TO_REMOVE
)

fire_calls = get_wprdc_data(
    resource_id='b6340d98-69a0-4965-a9b4-3480cea1182b',
    where_clauses=where_clauses,
    fields_to_remove=FIELDS_TO_REMOVE
)

json_to_gcs('ems/{}/{}/{}_ems.json'.format(args['execution_date'].split('-')[0],
                                           args['execution_date'].split('-')[1], args['execution_date']),
            ems_calls, bucket)

json_to_gcs('fire/{}/{}/{}_fire.json'.format(args['execution_date'].split('-')[0],
                                             args['execution_date'].split('-')[1], args['execution_date']),
            fire_calls, bucket)
