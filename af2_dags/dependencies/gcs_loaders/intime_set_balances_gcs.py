import os
import io
import csv
import json
import argparse
import pendulum

from datetime import datetime
from requests.auth import HTTPBasicAuth
from google.cloud import storage
from gcs_utils import generate_xml, post_xml, json_to_gcs, send_alert_email_with_csv

storage_client = storage.Client()

parser = argparse.ArgumentParser()
parser.add_argument('--output_arg', dest='out_loc', required=True,
                    help='fully specified location to upload the update log')
args = vars(parser.parse_args())

today = datetime.now(tz=pendulum.timezone("utc")).strftime("%Y-%m-%d")

BASE_URL = 'https://intime2.intimesoft.com/ise/timebank/TimeBankService'
auth = HTTPBasicAuth(os.environ['INTIME_USER'], os.environ['INTIME_PW'])
soap_url = 'timebank.export.attendance.bo'
prefix = 'tns'
request = 'setBalance'
headers = {'Content-Type': 'application/xml'}
start = f'<setBalanceResponse xmlns:ns2="http://{soap_url}.rise.intimesoft.com/">'
end = '</setBalanceResponse>'

bucket = storage_client.get_bucket(f"{os.environ['GCS_PREFIX']}_intime")
blob = bucket.blob("timebank/time_balance_mismatches.csv")
content = blob.download_as_string()
stream = io.StringIO(content.decode(encoding='utf-8'))

# DictReader used over Pandas Dataframe for fast iteration + minimal memory usage
update_log = []
if json.loads(os.environ['USE_PROD_RESOURCES'].lower()):
    csv_reader = csv.DictReader(stream, delimiter=',')
    for row in csv_reader:
        params = [{'tag': 'employeeId', 'content': row['Employee ID']},
                  {'tag': 'timeBankRef', 'content': row['Time Bank Reference']},
                  {'tag': 'date', 'content': row['Set Balance Date']},
                  {'tag': 'hours', 'content': float(row['Balance'])}]

        response = post_xml(BASE_URL, envelope=generate_xml(soap_url, request, params, prefix=prefix),
                            auth=auth, headers=headers)

        # An empty response dictionary indicates that the update failed. Otherwise, print update details to the console.
        if response != {'root': {'return': None}}:
            upd_row = dict(row)
            print(f"Successfully updated {upd_row['Time Bank Reference']} time bank balance for employee "
                  f"#{upd_row['Employee ID']} to {float(upd_row['Balance'])} for date {row['Set Balance Date']}")
            update_log.append(upd_row)
        else:
            print(f"Update operation failed for employee #{row['Employee ID']} with time bank code "
                  f"{row['Time Bank Reference']} for date {row['Set Balance Date']}")
else:
    print('No update performed')

if update_log:
    json_to_gcs(f"{args['out_loc']}", update_log, f"{os.environ['GCS_PREFIX']}_intime")
    send_alert_email_with_csv("osar@pittsburghpa.gov", "ALERT: InTime Time Banks Updated",
                              "The attached CSV lists all updates that have been made to time bank balances in the "
                              "InTime source system using information found in Dayforce. The balances are correct as "
                              "of the listed dates, and update operation did not overwrite any time accrued afterward.",
                              update_log, "time_bank_update_log.csv")
