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


run_start_win, first_run = find_last_successful_run(bucket, "requests/backfill/backfill_log/log.json", DEFAULT_RUN_START)
run_stop_win = last_day_of_month(run_start_win)

# adapted from https://pynative.com/python-get-last-day-of-month/#h-get-last-day-of-a-previous-month
input_dt = datetime.today()
curr_month_first = input_dt.replace(day=1)
curr_month_first = str(curr_month_first.strftime("%Y-%m-%d")) + " 00:00:00"

if run_start_win != curr_month_first:
    # qscend API requires a value (any value) for the user-agent field
    headers = {'User-Agent': 'City of Pittsburgh ETL'}
    payload = {'key': os.environ['QALERT_KEY'], 'start': run_start_win, 'end': run_stop_win}

    # API call to get data
    response = requests.get('https://pittsburghpa.qscend.com/qalert/api/v1/requests/dump', params=payload,
                            headers=headers)
    curr_run = datetime.now(tz=pendulum.timezone('UTC')).strftime("%Y-%m-%d %H:%M:%S")
    print("Response at " + str(curr_run) + ": " + str(response.status_code))

    # convert the utf-8 encoded string to json
    full_requests = response.json()['request']

    run_stop_dt = datetime.strptime(run_stop_win, "%Y-%m-%d %H:%M:%S")
    next_run = run_stop_dt + timedelta(days=1)
    next_run = str(next_run.strftime("%Y-%m-%d")) + " 00:00:00"

    # write the successful run information (used by each successive DAG run to find the backfill date)
    successful_run = [{"requests_retrieved": len(full_requests),
                       "since": run_start_win,
                       "current_run": next_run,
                       "note": "Data retrieved between the time points listed above"}]
    json_to_gcs("requests/backfill/backfill_log/log.json", successful_run, bucket)

    # load to gcs
    json_to_gcs(args["out_loc"], full_requests, bucket)

else:
    print("No new backfill to perform")
