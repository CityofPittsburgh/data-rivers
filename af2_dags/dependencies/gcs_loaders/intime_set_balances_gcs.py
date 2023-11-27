import os
import io
import csv
import pendulum

from datetime import datetime
from requests.auth import HTTPBasicAuth
from google.cloud import storage
from gcs_utils import generate_xml, post_xml

storage_client = storage.Client()

today = datetime.now(tz=pendulum.timezone("utc")).strftime("%Y-%m-%d")

BASE_URL = 'https://intime2.intimesoft.com/ise/timebank/TimeBankService'
auth = HTTPBasicAuth(os.environ['INTIME_USER'], os.environ['INTIME_PW'])
soap_url = 'timebank.export.attendance.bo'
prefix = 'tns'
request = 'setBalance'
headers = {'Content-Type': 'application/xml'}
start = f'<setBalanceResponse xmlns:ns2="http://{soap_url}.rise.intimesoft.com/">'
end = '</setBalanceResponse>'

bucket = storage_client.get_bucket(f"{os.environ['GCS_PREFIX']}_pbp")
blob = bucket.blob("data_sharing/time_balance_mismatches.csv")
content = blob.download_as_string()
stream = io.StringIO(content.decode(encoding='utf-8'))

# DictReader used over Pandas Dataframe for fast iteration + minimal memory usage
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
        print(f"Successfully updated {row['code']} time bank balance for employee #{row['employee_id']} from "
              f"{float(row['intime_balance'])} to {float(row['ceridian_balance'])}")
    else:
        print(f"Update operation failed for employee #{row['employee_id']} with time bank code {row['code']}")


