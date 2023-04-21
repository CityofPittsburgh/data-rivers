import os
import argparse
# import pendulum
# from datetime import datetime, timedelta
import requests
# from requests.auth import HTTPBasicAuth
from google.cloud import storage

from gcs_utils import json_to_gcs
#, find_last_successful_run

API_LIMIT = 5000

storage_client = storage.Client()
bucket = f"{os.environ['GCS_PREFIX']}_eproperty"

parser = argparse.ArgumentParser()
parser.add_argument('--output_arg', dest='out_loc', required=True,
                    help='fully specified location to upload the combined ndjson file')
args = vars(parser.parse_args())


URL_BASE = F"https://api.epropertyplus.com/landmgmt/api/"
URL_QUERY = F""


# GET /landmgmt/api/referenceData/list/Property%20Statusx HTTP/1.1
header = {"Host": "api.epropertyplus.com",
"Connection": "keep - alive",
"x - strllc - authkey": F"os.environ['E_PROPERTY_KEY']",
"User - Agent": "Mozilla / 5.0(Macintosh; Intel Mac OS X 10_9_2...",
"Content - Type": "application / json",
"Accept": "* / *",
"Accept - Encoding": "gzip, deflate, sdch",
"Accept - Language": "en - US, en;q = 0.8"}


all_records = []
url = URL_BASE + URL_QUERY
# Make use of session to retain authorization header when a URL redirect happens
more = True
while more is True:
    # API call to get data
    response = requests.get(url)
    if response.status_code == 200:
        #     blah
        res = response
        # .blah
        # append list of API results to growing all_records list
        all_records.append(res)
    else:
        # error_notification
    if more:
    #     blah
    else:
        # break out of While Loop...no more records
        more = False




    # write the successful run information (used by each successive run to find the backfill start date)

json_to_gcs(f"{args['out_loc']}", all_records, bucket)