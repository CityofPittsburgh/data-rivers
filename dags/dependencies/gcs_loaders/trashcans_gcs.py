# setup library imports
import io
import time
import json
import os
import argparse
import copy

import requests
import pandas as pd
from gcs_utils import filter_fields, json_to_gcs

parser = argparse.ArgumentParser()
parser.add_argument('-e', '--execution_date', dest='execution_date',
                    required=True, help='DAG execution date (YYYY-MM-DD)')
args = vars(parser.parse_args())
execution_date = args['execution_date']
bucket = '{}_trash_cans'.format(os.environ['GCS_PREFIX'])


BASE_URL = "https://relayapijson.victorstanley.com"

# list of attributes to remove

CONTAINERS_COLUMNS = ['AssignmentDate','Bin2Content','CalculatedBin2PercentFull','CalculatedBin2PercentFullBeforeCollectionEvent',
                'CellSignalStrength','InitializationDate', 'IsCollectionEventForBin2','IsHighestThresholdLevelReachedForBin2',
                'IsThresholdLevelReachedForBin2', 'LastCheckinDateBeforeCollectionEventForBin2',
                'LastTimeDoor1Opened','LastTimeDoor2Opened','LastWeight2BeforeCollectionEvent','State','Weight2']

CONTAINERS_HISTORY_DATA_COLUMNS=['Bin2Content', 'CalculatedBin2PercentFull','CalculatedBin2PercentFullBeforeCollectionEvent', 'CellSignalStrength',
                          'IsCollectionEventForBin2', 'IsHighestThresholdLevelReachedForBin2','IsThresholdLevelReachedForBin2','LastCheckinDateBeforeCollectionEventForBin2',
                           'LastTimeDoor2Opened', 'LastWeight2BeforeCollectionEvent','State', 'Weight2']

GROUPS_DATA_COLUMNS=['SpikeEmailAlertThreshold']

ALERT_DATA_HISTORY_COLUMNS=['MeId', 'LastUpdatedDate', 'DismissedDate', 'DismissedByEmail']

#endpoints is a list of dictionaries used by the upload_trash_can_data function to push API data to associated buckets
endpoints = [{'path': 'ContainersData', 'date_param': True, 'filter_list': CONTAINERS_COLUMNS},
             {'path': 'GroupsData', 'date_param': False, 'filter_list': GROUPS_DATA_COLUMNS},
             {'path':'ContainersHistoryData','date_param': True , 'filter_list': CONTAINERS_HISTORY_DATA_COLUMNS},
            {'path':'GroupsUsersData','date_param': False , 'filter_list': None },
            {'path':'ContainerConfiguration','date_param': False, 'filter_list': None},
            {'path':'GroupUserGroups','date_param': False, 'filter_list': None},
            {'path':'BinContent','date_param': False, 'filter_list': None},
            {'path':'ContainerStatus','date_param': False, 'filter_list': None},
            {'path':'AlertDataHistory','date_param': True, 'filter_list': ALERT_DATA_HISTORY_COLUMNS}]

# reformats exection date passed from command line argument to one that is required by GCS
def get_api_date(execution_date):
    temp= execution_date.split("-")
    return temp[1]+"-"+temp[2]+"-"+temp[0]

def upload_trash_can_data(endpoints):
    for endpoint in endpoints:
        if endpoint['date_param']:
            res = requests.get(
                BASE_URL + '/' + endpoint['path'] + '/' + os.environ['TRASH_CAN_KEY'] + '/' + get_api_date(
                    execution_date))
        else:
            res = requests.get(BASE_URL + '/' + endpoint['path'] + '/' + os.environ['TRASH_CAN_KEY'])
        if endpoint['filter_list']:

            data = filter_fields(res.json(), relevant_fields=endpoint['filter_list'], add_fields = False)

            json_to_gcs('{}/{}/{}/{}_{}.json'.format(endpoint['path'].lower(), execution_date.split('-')[0],
                                                     execution_date.split('-')[1], execution_date, endpoint['path'].lower()), data, bucket)

        else:
            json_to_gcs('{}/{}/{}/{}_{}.json'.format(endpoint['path'].lower(), execution_date.split('-')[0],
                                                     execution_date.split('-')[1], execution_date, endpoint['path'].lower()), data, bucket)



upload_trash_can_data(endpoints)


