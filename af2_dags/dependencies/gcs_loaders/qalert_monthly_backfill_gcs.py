import os
import argparse
import pendulum
import requests
from datetime import datetime, timedelta
from google.cloud import storage

from gcs_utils import json_to_gcs, find_last_successful_run

# init the GCS client for compose operation (to append each batch of data to a growing json)
storage_client = storage.Client()
bucket = f"{os.environ['GCS_PREFIX']}_qalert"

# init the backfill constants (qalert system went online 4/20/15)
DEFAULT_RUN_START = "2015-04-19 00:00:00"
# DEFAULT_RUN_STOP = "2015-04-30 23:59:59"

parser = argparse.ArgumentParser()
parser.add_argument('--output_arg', dest='out_loc', required=True,
                    help='fully specified location to upload the combined ndjson file')
args = vars(parser.parse_args())


# adapted from https://stackoverflow.com/a/43088
def last_day_of_month(datestring):
    input_date = datetime.strptime(datestring, "%Y-%m-%d %H:%M:%S")
    if input_date.month == 12:
        return str(input_date.replace(day=31).strftime("%Y-%m-%d")) + " 23:59:59"
    date_val = input_date.replace(month=input_date.month+1, day=1) - timedelta(days=1)
    return str(date_val.date().strftime("%Y-%m-%d")) + " 23:59:59"


run_start_win, first_run = find_last_successful_run(bucket, "requests/successful_run_log/log.json", DEFAULT_RUN_START)
run_stop_win = last_day_of_month(run_start_win)

# adapted from https://pynative.com/python-get-last-day-of-month/#h-get-last-day-of-a-previous-month
# input_dt = datetime.today()
# curr_month_first = input_dt.replace(day=1)
# month_last = curr_month_first - timedelta(days=1)
# month_first = month_last.replace(day=1)
#
# start = str(month_first.strftime("%Y-%m-%d") + "00:00:00")
# stop = str(month_last.strftime("%Y-%m-%d") + "23:59:59")

# qscend API requires a value (any value) for the user-agent field
headers = {'User-Agent': 'City of Pittsburgh ETL'}
payload = {'key': os.environ['QALERT_KEY'], 'since': run_start_win, 'end': run_stop_win}
