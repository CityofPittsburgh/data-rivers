import os
import argparse

from gcs_utils import json_to_gcs, get_wprdc_data, swap_field_names

"""
This DAG runs monthly, with a query downstream to filter duplicates from the final table. Note that as with call date,
location data is obscured to census block lat/long for privacy reasons. 
"""

parser = argparse.ArgumentParser()
parser.add_argument('-e', '--execution_date', dest='execution_date',
                    required=True, help='DAG execution date (YYYY-MM-DD)')
args = vars(parser.parse_args())

bucket = '{}_ems_fire'.format(os.environ['GCS_PREFIX'])

RELEVANT_KEYS = [
                 '_id',
                 'service',
                 'call_quarter',
                 'call_year',
                 'description_short',
                 'priority',
                 'priority_desc',
                 'call_id_hash',
                 'census_block_group_center__x',
                 'census_block_group_center__y'
                ]


def execution_date_to_prev_quarter(execution_date):
    """
    For privacy reasons, these tables are rolled up to quarter rather exact date, and are only released once a quarter
    concludes. Consequently, this script pulls data for the most recently concluded quarter.

    :param execution_date: DAG execution date, passed through via Airflow template variable
    :return: quarter, year as int (e.g. 'Q1', 2020)
    """

    split_date = execution_date.split('-')
    year = split_date[0]
    day = split_date[1] + '-' + split_date[2]
    if '01-01' <= day < '04-01':
        quarter = 'Q4'
        year = int(year) - 1
    elif '04-01' <= day < '07-01':
        quarter = 'Q1'
    elif '07-01' <= day < '10-01':
        quarter = 'Q2'
    else:
        quarter = 'Q3'

    return quarter, int(year)


def filter_call_keys(calls):
    """
    take results from API and remove keys we don't need, rename lat/long fields

    :param calls: list of dicts
    :return: transformed list of dicts
    """
    trimmed_calls = []
    for call in calls:
        trimmed_call = {k: call[k] for k in RELEVANT_KEYS}
        trimmed_call = swap_field_names(trimmed_call, [("census_block_group_center__x", "long"),
                                                       ("census_block_group_center__y", "lat")])
        trimmed_calls.append(trimmed_call)

    return trimmed_calls


quarter, year = execution_date_to_prev_quarter(args['execution_date'])

where_clauses = '"city_name" = \'PITTSBURGH\' AND "call_quarter" = \'{}\' ' \
                'AND "call_year" = \'{}\''.format(quarter, year)

ems_calls = get_wprdc_data(
    resource_id='ff33ca18-2e0c-4cb5-bdcd-60a5dc3c0418',
    where_clauses=where_clauses
)

fire_calls = get_wprdc_data(
    resource_id='b6340d98-69a0-4965-a9b4-3480cea1182b',
    where_clauses=where_clauses
)

# filter responses to take out unnecessary keys, preserving only those we've defined in RELEVANT_KEYS

json_to_gcs('ems/{}/{}/{}_ems.json'.format(args['execution_date'].split('-')[0],
                                           args['execution_date'].split('-')[1], args['execution_date']),
            filter_call_keys(ems_calls), bucket)

json_to_gcs('fire/{}/{}/{}_fire.json'.format(args['execution_date'].split('-')[0],
                                             args['execution_date'].split('-')[1], args['execution_date']),
            filter_call_keys(fire_calls), bucket)
