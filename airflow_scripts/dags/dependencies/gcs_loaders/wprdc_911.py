import os
import argparse

from gcs_utils import json_to_gcs, get_wprdc_data


parser = argparse.ArgumentParser()
parser.add_argument('-e', '--execution_date', dest='execution_date',
                    required=True, help='DAG execution date (YYYY-MM-DD)')
args = vars(parser.parse_args())

bucket = '{}_911'.format(os.environ['GCS_PREFIX'])

RELEVANT_KEYS = ['service',
                 'priority',
                 'priority_desc',
                 'call_quarter',
                 'call_year',
                 'description_short',
                 'census_block_group_center__x',
                 'census_block_group_center__y']


def execution_date_to_quarter(execution_date):
    split_date = execution_date.split('-')
    year = split_date[0]
    day = split_date[1] + '-' + split_date[2]
    if '01-01' <= day < '04-01':
        quarter = 'Q1'
    elif '04-01' <= day < '07-01':
        quarter = 'Q2'
    elif '07-01' <= day < '10-01':
        quarter = 'Q3'
    else:
        quarter = 'Q4'

    return quarter, int(year)


"""
These calls are grouped only by year/quarter for privacy reasons, so we convert the DAG execution date to quarter/year
and pull the current calls that way. the DAG later runs a query to filter duplicates from the final BigQuery table.
Location data is obscured to census block lat/long. 
"""

quarter, year = execution_date_to_quarter(args['execution_date'])

ems_calls = get_wprdc_data(
    'ff33ca18-2e0c-4cb5-bdcd-60a5dc3c0418',
    where_clauses=['"city_name" = \'PITTSBURGH\'',
                   '"call_quarter" = \'{}\''.format(quarter),
                   '"call_year" = \'{}\''.format(year)]
)

fire_calls = get_wprdc_data(
    'b6340d98-69a0-4965-a9b4-3480cea1182b',
    where_clauses=['"city_name" = \'PITTSBURGH\'',
                   '"call_quarter" = \'{}\''.format(quarter),
                   '"call_year" = \'{}\''.format(year)]
)

trimmed_ems = []
trimmed_fire = []

# filter responses to take out unnecessary keys, preserving only those we've defined in RELEVANT_KEYS
for call in ems_calls:
    trimmed_call = {k: call[k] for k in RELEVANT_KEYS}
    trimmed_ems.append(trimmed_call)

for call in fire_calls:
    trimmed_call = {k: call[k] for k in RELEVANT_KEYS}
    trimmed_fire.append(trimmed_call)


json_to_gcs('ems/{}/{}/{}_ems.json'.format(args['execution_date'].split('-')[0],
                                           args['execution_date'].split('-')[1], args['execution_date']),
            trimmed_ems, bucket)

json_to_gcs('fire/{}/{}/{}_fire.json'.format(args['execution_date'].split('-')[0],
                                             args['execution_date'].split('-')[1], args['execution_date']),
            trimmed_fire, bucket)

"""
>>> gcs_utils.get_wprdc_data('ff33ca18-2e0c-4cb5-bdcd-60a5dc3c0418', where_clauses=['"city_name" = 
\'PITTSBURGH\''], limit=5) [

{'_geom': None, 'call_year': 2015, 'service': 'EMS', 'call_quarter': 'Q4', 
'priority_desc': 'EMS Advanced Life Support life threatening response', 'census_block_group_center__x': 
-79.9197650331037, 'priority': 'E1', 'census_block_group_center__y': 40.4687334135596, 'call_id_hash': 
'001EDF9979F3584AE8DB1B61491414', 'city_name': 'PITTSBURGH', '_id': 446, 'description_short': 'SICK', 
'geoid': '420031113004006', 'city_code': 'PGH', '_the_geom_webmercator': None}, {'_geom': None, 'call_year': 2018, 
'service': 'EMS', 'call_quarter': 'Q3', 'priority_desc': 'EMS Advanced Life Support life threatening response', 
'census_block_group_center__x': -79.8905441779029, 'priority': 'E1', 'census_block_group_center__y': 
40.4898480379435, 'call_id_hash': '3411DA80FBA62C42F7B441AF6F81EE', 'city_name': 'PITTSBURGH', '_id': 197683, 
'description_short': 'DIABETIC', 'geoid': '420039800001001', 'city_code': 'PGH', '_the_geom_webmercator': None}, 
{'_geom': None, 'call_year': 2017, 'service': 'EMS', 'call_quarter': 'Q2', 'priority_desc': 'EMS Standard Basic Life 
Support response', 'census_block_group_center__x': -79.9002839572352, 'priority': 'E3', 
'census_block_group_center__y': 40.4524411686907, 'call_id_hash': '00226FD56C0539CF83B01EE9C1EC53', 'city_name': 
'PITTSBURGH', '_id': 458, 'description_short': 'RQST ASST  EMS', 'geoid': '420031405001013', 'city_code': 'PGH', 
'_the_geom_webmercator': None}, {'_geom': None, 'call_year': 2017, 'service': 'EMS', 'call_quarter': 'Q1', 
'priority_desc': 'EMS Advanced Life Support life threatening response', 'census_block_group_center__x': 
-80.0631620886948, 'priority': 'E1', 'census_block_group_center__y': 40.4273038465795, 'call_id_hash': 
'3744F0D732E9C70AD5129C8FB733AF', 'city_name': 'PITTSBURGH', '_id': 501749, 'description_short': 'BREATHING', 
'geoid': '420035628002013', 'city_code': 'PGH', '_the_geom_webmercator': None}, {'_geom': None, 'call_year': 2019, 
'service': 'EMS', 'call_quarter': 'Q3', 'priority_desc': 'EMS Advanced Life Support life threatening response', 
'census_block_group_center__x': -79.9461867472698, 'priority': 'E1', 'census_block_group_center__y': 
40.4541700033649, 'call_id_hash': '37442BCFE2F7214DDED1FDF8D99BCA', 'city_name': 'PITTSBURGH', '_id': 233786, 
'description_short': 'SICK', 'geoid': '420030804002008', 'city_code': 'PGH', '_the_geom_webmercator': None}] 
"""
