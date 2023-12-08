import os
import io
import json
import argparse

import pandas as pd
from requests.auth import HTTPBasicAuth
from datetime import datetime
from google.cloud import storage
from gcs_utils import generate_xml, post_xml, json_to_gcs, get_last_upload, get_ceridian_report, \
    find_last_successful_run


storage_client = storage.Client()
today = datetime.today()
DEFAULT_RUN_START = '2022-12-04'

parser = argparse.ArgumentParser()
parser.add_argument('--ceridian_output', dest='ceridian_output', required=True,
                    help='fully specified location to upload the combined Ceridian ndjson file')
parser.add_argument('--intime_output', dest='intime_output', required=True,
                    help='fully specified location to upload the combined InTime ndjson file')
args = vars(parser.parse_args())

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
i_bucket_name = f"{os.environ['GCS_PREFIX']}_intime"
i_bucket = storage_client.get_bucket(i_bucket_name)
log_path = 'employees/successful_run_log/log.json'
known_path = 'employees/2023/08/17/scheduled__2023-08-17T19:00:00+00:00_records.json'
last_upload = get_last_upload(i_bucket_name, log_path, known_path)

c_bucket_name = f"{os.environ['GCS_PREFIX']}_ceridian"
c_bucket = storage_client.get_bucket(c_bucket_name)
blob = c_bucket.blob("timekeeping/payroll_schedule_23-24.csv")
content = blob.download_as_string()
stream = io.StringIO(content.decode(encoding='utf-8'))
sched_df = pd.read_csv(stream)
sched_df['pay_period_end'] = pd.to_datetime(sched_df['pay_period_end'])

run_start_win, first_run = find_last_successful_run(i_bucket_name, "timebank/backfill/backfill_log/log.json", DEFAULT_RUN_START)
backfill_start = datetime.strptime(run_start_win, '%Y-%m-%d')
sched_df = sched_df[(backfill_start < sched_df['pay_period_end']) & (sched_df['pay_period_end'] < today)]

cw_bucket = storage_client.get_bucket("user_defined_data")
cw_blob = cw_bucket.get_blob("timebank_codes.json")
cw = cw_blob.download_as_string()
ref_codes = json.loads(cw.decode('utf-8'))

ceridian_data = []
intime_data = []
for index, row in enumerate(sched_df.itertuples(), 1):
    params = f'a9b23df9-0bbb-4ada-9669-f3432b741da3={row.pay_period_end.strftime("%m/%d/%Y %H:%M:%S")}'
    params += f'&4244386a-b503-4b9a-8843-70b00ad70259={row.pay_period_end.strftime("%m/%d/%Y")} 23:59:59'
    ceridian_records = get_ceridian_report(f'ACCRUALSREPORTPOLICE?{params}')
    ceridian_records = [{**rec, 'date': row.pay_period_end.strftime("%Y-%m-%d")} for rec in ceridian_records]
    ceridian_data.extend(ceridian_records)
    # for value in last_upload:
    #     for code in ref_codes:
    #         params = [{'tag': 'employeeId', 'content': value['employeeId']},
    #                   {'tag': 'timeBankRef', 'content': ref_codes[code]},
    #                   {'tag': 'date', 'content': row.pay_period_end.strftime("%Y-%m-%d")}]
    #
    #         # API requests need to be made one at a time, which may overwhelm the request limit.
    #         # Within the post_xml util, the request attempts in a loop until a 200/500 response is obtained
    #         response = post_xml(BASE_URL, envelope=generate_xml(soap_url, request, params, prefix=prefix),
    #                             auth=auth, headers=headers, res_start=start, res_stop=end)
    #         balance = response['root']['return']
    #         if balance:
    #             record = {'employee_id': value['employeeId'], 'date': row.pay_period_end.strftime("%Y-%m-%d"),
    #                       'time_bank': code, 'code': ref_codes[code], 'balance': balance}
    #             intime_data.append(record)
    #         else:
    #             print(f'No balance for employee #{value["employeeId"]} in time bank {ref_codes[code]} on date {row.pay_period_end.strftime("%Y-%m-%d")}')


print(f"Count of backfilled Cerdian records: {len(ceridian_data)}")
json_to_gcs(f"{args['ceridian_output']}", ceridian_data, c_bucket_name)

print(f"Count of backfilled InTime records: {len(intime_data)}")
json_to_gcs(f"{args['intime_output']}", intime_data, i_bucket_name)

successful_run = [{"requests_retrieved": len(ceridian_records) + len(intime_data),
                   "since": run_start_win,
                   "current_run": today.strftime("%Y-%m-%d"),
                   "note": "Data retrieved between the time points listed above"}]
json_to_gcs("timebank/backfill/backfill_log/log.json", successful_run, i_bucket_name)
