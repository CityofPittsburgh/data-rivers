import os
import argparse
import pendulum

from datetime import datetime
from requests.auth import HTTPBasicAuth
from google.cloud import storage
from gcs_utils import json_to_gcs, get_last_upload, generate_xml, post_xml


# Initialize the GCS client
client = storage.Client()

parser = argparse.ArgumentParser()
parser.add_argument('--output_arg', dest='out_loc', required=True,
                    help='fully specified location to upload the combined ndjson file')
args = vars(parser.parse_args())

today = datetime.now(tz=pendulum.timezone("utc")).strftime("%Y-%m-%d")

BASE_URL = 'https://intime2.intimesoft.com/ise/timebank/TimeBankService'
auth = HTTPBasicAuth(os.environ['INTIME_USER'], os.environ['INTIME_PW'])
soap_url = 'timebank.export.attendance.bo'
prefix = 'tns'
request = 'getBalance'
headers = {'Content-Type': 'application/xml'}

# These variables allow us to parse the record information from the XML text returned by the API
start = f'<ns2:getBalanceResponse xmlns:ns2="http://{soap_url}.rise.intimesoft.com/">'
end = '</ns2:getBalanceResponse>'

# Retrieve path of most recent InTime employees extract - use that as source for time bank data
bucket_name = f"{os.environ['GCS_PREFIX']}_intime"
bucket = client.get_bucket(bucket_name)
log_path = 'employees/successful_run_log/log.json'
known_path = 'employees/2023/08/17/scheduled__2023-08-17T19:00:00+00:00_records.json'
last_upload = get_last_upload(bucket_name, log_path, known_path)

ref_codes = [('COVAC', 'Vacation Carried Over - Hours'), ('COMP', 'Comp Time - Hours'),
             ('Military', 'Military - Hours'), ('Personal', 'Personal - Hours'), ('VAC', 'Vacation - Hours'),
             ('DH', 'Deferred Holiday - Hours'), ('PPL', 'Parental Leave - Hours'), ('SICK', 'Sick Legacy - Hours')]
data = []
for row in last_upload:
    for code in ref_codes:
        params = [{'tag': 'employeeId', 'content': row['employeeId']},
                  {'tag': 'timeBankRef', 'content': code[0]},
                  {'tag': 'date', 'content': today}]

        # API requests need to be made one at a time, which may overwhelm the request limit.
        # Within the post_xml util, the request attempts in a loop until a 200/500 response is obtained
        response = post_xml(BASE_URL, envelope=generate_xml(soap_url, request, params, prefix=prefix),
                            auth=auth, headers=headers, res_start=start, res_stop=end)
        balance = response['root']['return']
        if balance:
            record = {'employee_id': row['employeeId'], 'date': today, 'time_bank': code[1], 'balance': balance}
            data.append(record)
        else:
            print(f'No balance for employee #{row["employeeId"]} in time bank {code[1]}')

json_to_gcs(args['out_loc'], data, bucket)
