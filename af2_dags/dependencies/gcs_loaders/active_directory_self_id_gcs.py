import os
import io
import csv
import json
import argparse
import pendulum

from datetime import datetime
from google.cloud import storage
from gcs_utils import send_alert_email_with_csv

storage_client = storage.Client()

parser = argparse.ArgumentParser()
parser.add_argument('--output_arg', dest='out_loc', required=True,
                    help='fully specified location to upload the update log')
args = vars(parser.parse_args())

today = datetime.now(tz=pendulum.timezone("utc")).strftime("%Y-%m-%d")

bucket = storage_client.get_bucket(f"{os.environ['GCS_PREFIX']}_active_directory")
blob = bucket.blob("self_id/first_name_comp000000000000.csv")
content = blob.download_as_string()
stream = io.StringIO(content.decode(encoding='utf-8'))

# DictReader used over Pandas Dataframe for fast iteration + minimal memory usage
update_log = []
if json.loads(os.environ['USE_PROD_RESOURCES'].lower()):
    csv_reader = csv.DictReader(stream, delimiter=',')
    for row in csv_reader:
        send_alert_email_with_csv([row['ad_email']], [os.environ['EMAIL']],
                                  "Action Needed: Complete Form With Your Preferred First Name",
                                  f"https://forms.office.com/g/{os.environ['AD_FNAME_SURVEY']}",
                                  update_log, f"time_bank_import_{today}.csv")
