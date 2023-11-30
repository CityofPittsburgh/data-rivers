import os
import io
import csv
import json
import argparse
import pendulum

from datetime import datetime
from requests.auth import HTTPBasicAuth
from google.cloud import storage
from gcs_utils import generate_xml, post_xml, json_to_gcs

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

bucket = storage_client.get_bucket(f"{os.environ['GCS_PREFIX']}_ceridian")
blob = bucket.blob("data_sharing/time_balance_mismatches.csv")
content = blob.download_as_string()
stream = io.StringIO(content.decode(encoding='utf-8'))

# DictReader used over Pandas Dataframe for fast iteration + minimal memory usage
update_log = []
if json.loads(os.environ['USE_PROD_RESOURCES'].lower()):
    csv_reader = csv.DictReader(stream, delimiter=',')
    for row in csv_reader:
        params = [{'tag': 'employeeId', 'content': row['employee_id']},
                  {'tag': 'timeBankRef', 'content': row['code']},
                  {'tag': 'date', 'content': today},
                  {'tag': 'hours', 'content': float(row['ceridian_balance'])}]

        response = post_xml(BASE_URL, envelope=generate_xml(soap_url, request, params, prefix=prefix),
                            auth=auth, headers=headers, res_start=start, res_stop=end)

        # An empty response dictionary indicates that the update failed. Otherwise, print update details to the console.
        if response['root']['return']:
            upd_row = dict(row)
            upd_row['old_balance'] = upd_row.pop('intime_balance')
            upd_row['new_balance'] = upd_row.pop('ceridian_balance')
            print(f"Successfully updated {upd_row['code']} time bank balance for employee #{upd_row['employee_id']} from "
                  f"{float(upd_row['old_balance'])} to {float(upd_row['new_balance'])}")
            update_log.append(upd_row)
        else:
            print(f"Update operation failed for employee #{row['employee_id']} with time bank code {row['code']}")
            print(f"Could not update InTime balance from {row['intime_balance']} to {row['ceridian_balance']}")

if update_log:
    json_to_gcs(f"{args['out_loc']}", update_log, f"{os.environ['GCS_PREFIX']}_intime")
