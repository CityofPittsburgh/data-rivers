import os
import argparse

from gcs_utils import storage_client, json_to_gcs, get_wprdc_data, time_to_seconds


def execution_date_to_quarter(execution_date):
    split_date = execution_date.split('-')
    day = split_date[1] + '-' + split_date[2]
    if '01-01' <= day < '04-01':
        quarter = 'Q1'
    elif '04-01' <= day < '07-01':
        quarter = 'Q2'
    elif '07-01' <= day < '10-01':
        quarter = 'Q3'
    else:
        quarter = 'Q4'

    return quarter

