import os
import argparse

from gcs_utils import json_to_gcs, get_wprdc_data


parser = argparse.ArgumentParser()
parser.add_argument('-e', '--execution_date', dest='execution_date',
                    required=True, help='DAG execution date (YYYY-MM-DD)')
parser.add_argument('-p', '--prev_execution_date', dest='prev_execution_date',
                    required=True, help='Prev DAG execution date (YYYY-MM-DD)')
args = vars(parser.parse_args())

bucket = '{}_parking'.format(os.environ['GCS_PREFIX'])

parking_meters = get_wprdc_data(
    resource_id='9ed126cc-3c06-496e-bd08-b7b6b14b4109',
    fields_to_remove=["_geom", "guid", "_the_geom_webmercator"]
)   

parking_transactions = get_wprdc_data(
    resource_id='1ad5394f-d158-46c1-9af7-90a9ef4e0ce1',
    where_clauses='"utc_start" >= \'{}\''.format(args['prev_execution_date']),
    fields_to_remove=["_geom", "_the_geom_webmercator"]
)

json_to_gcs('meters/{}/{}/{}_meters.json'.format(args['execution_date'].split('-')[0],
                                                 args['execution_date'].split('-')[1],
                                                 args['execution_date']),
            parking_meters, bucket)

json_to_gcs('transactions/{}/{}/{}_transactions.json'.format(args['execution_date'].split('-')[0],
                                                             args['execution_date'].split('-')[1],
                                                             args['execution_date']),
            parking_transactions, bucket)
