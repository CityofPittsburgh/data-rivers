import os
import argparse

from gcs_utils import json_to_gcs, get_wprdc_data, swap_field_names,\
    change_data_types

"""
"""

parser = argparse.ArgumentParser()
parser.add_argument('-e', '--execution_date', dest = 'execution_date',
                    required = True, help = 'DAG execution date (YYYY-MM-DD)')
args = vars(parser.parse_args())

bucket = '{}_parking'.format(os.environ['GCS_PREFIX'])

parking_meters = get_wprdc_data(
    resource_id = '9ed126cc-3c06-496e-bd08-b7b6b14b4109', fields_to_remove=[
        "_geom", "guid", "_the_geom_webmercator"])

field_names_to_swap = [("longitude", "long"), ("latitude", "lat")]
type_changes = [("long", "float"), ("lat", "float")]
cleaned_meters = []

for meter in parking_meters:
    cleaned_meter = swap_field_names(meter, field_names_to_swap)
    cleaned_meter = change_data_types(meter, type_changes)
    cleaned_meters.append(cleaned_meter)

json_to_gcs('meters/{}/{}/{}_meters.json'.format(args['execution_date'].split('-')[0],
                                                 args['execution_date'].split('-')[1],
                                                 args['execution_date']),
            cleaned_meters, bucket)
