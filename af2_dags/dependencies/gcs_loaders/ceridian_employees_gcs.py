import os
import argparse
import pendulum
from datetime import datetime, timedelta
from google.cloud import storage

from gcs_utils import json_to_gcs, find_last_successful_run, get_ceridian_report

API_LIMIT = 5000

storage_client = storage.Client()
bucket = f"{os.environ['GCS_PREFIX']}_ceridian"

parser = argparse.ArgumentParser()
parser.add_argument('--output_arg', dest='out_loc', required=True,
                    help='fully specified location to upload the combined ndjson file')
args = vars(parser.parse_args())

yesterday = datetime.combine(datetime.now(tz = pendulum.timezone("utc")) - timedelta(1),
                             datetime.min.time()).strftime("%Y-%m-%d %H:%M:%S")

# find the last successful DAG run (needs to be specified in UTC YYYY-MM-DD HH:MM:SS)
# if there was no previous good run, default to yesterday's date.
# this is used to initialize the payload below
run_start_win, first_run = find_last_successful_run(bucket, "employees/successful_run_log/log.json", yesterday)

all_records = get_ceridian_report(f"APIINFO?pageSize={API_LIMIT}")
# exclude records that were made for testing purposes
all_records = [r for r in all_records if r.get('Employee_LastName') != 'Test']

# write the successful run information (used by each successive run to find the backfill start date)
successful_run = {
    "requests_retrieved": len(all_records),
    "since": run_start_win,
    "current_run": datetime.now(tz = pendulum.timezone('UTC')).strftime("%Y-%m-%d %H:%M:%S"),
    "note": "Data retrieved between the time points listed above"
}
json_to_gcs("employees/successful_run_log/log.json", [successful_run],
            bucket)

json_to_gcs(f"{args['out_loc']}", all_records, bucket)
