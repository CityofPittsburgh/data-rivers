import os
import argparse
import pendulum
from datetime import datetime, timedelta
import requests
import json
import pytz
import tzlocal
import xmltodict
from google.cloud import storage

from gcs_utils import json_to_gcs, find_last_successful_run

DEFAULT_RUN_START = "2017-10-10T00:00:00Z"
INCREMENT_RECORDS = 100

storage_client = storage.Client()
bucket = f"{os.environ['GCS_PREFIX']}_chargepoint"

parser = argparse.ArgumentParser()
parser.add_argument('-l', '--lookback_date', dest='lookback_date',
                    required=True, help='Previous DAG execution date (YYYY-MM-DD)')
args = vars(parser.parse_args())

run_stop_win = DEFAULT_RUN_START

BASE_URL = 'https://webservices.chargepoint.com/webservices/chargepoint/services/5.0/'

def generate_xml(from_time, interval):
    return F"""
     <S:Envelope xmlns:S="http://schemas.xmlsoap.org/soap/envelope/" xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/">
        <SOAP-ENV:Header>
            <wsse:Security xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd" SOAP-ENV:mustUnderstand="1">
                <wsse:UsernameToken xmlns:wsu="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">
                    <wsse:Username>{os.environ['CHARGEPOINT_USER']}</wsse:Username>
                    <wsse:Password Type="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wssusername-token-profile-1.0#PasswordText">{os.environ['CHARGEPOINT_PW']}</wsse:Password>
                </wsse:UsernameToken>
            </wsse:Security>
        </SOAP-ENV:Header>
        <S:Body>
            <ns2:getChargingSessionData xmlns:ns2="urn:dictionary:com.chargepoint.webservices">
                <searchQuery>
                    <fromTimeStamp>{from_time}</fromTimeStamp>
                    <startRecord>{interval}</startRecord>
                </searchQuery>
            </ns2:getChargingSessionData>
        </S:Body>
    </S:Envelope>
    """


headers = {'Content-Type': 'application/soap+xml'}#, 'User-Agent': 'City of Pittsburgh ETL'}
interval = 1
start = '</responseText>'
end = '</ns1:getChargingSessionDataResponse>'

#while datetime.strptime(run_stop_win, "%Y-%m-%dT%H:%M:%SZ") <= datetime.strptime(args['lookback_date'], "%Y-%m-%d"):
run_start_win, first_run = find_last_successful_run(bucket, "successful_run_log/log.json", DEFAULT_RUN_START)
start_time = run_start_win
more = True
while more is True:
    # API call to get data
    response = requests.post(BASE_URL, data=generate_xml(run_start_win, interval), headers=headers)
    vals = response.text[response.text.find(start) + len(start):response.text.rfind(end)]
    vals = '<root>' + vals + '</root>'
    xml_dict = xmltodict.parse(xml_input=vals, encoding='utf-8')
    if xml_dict['root']['MoreFlag']:
        more = (xml_dict['root']['MoreFlag'] == '1')
    else:
        more = False
    records = xml_dict['root']['ChargingSessionData']
    end_time = records[len(records)-1]['endTime']
    interval += INCREMENT_RECORDS

    # write the successful run information (used by each successive run to find the backfill start date)
    successful_run = {
        "requests_retrieved": len(records),
        "since": run_start_win,
        "current_run": run_stop_win,
        "note": "Data retrieved between the time points listed above"
    }
    json_to_gcs("successful_run_log/log.json", [successful_run],
                bucket)

    # each run of data extracted from the API will be appended to a growing JSON and saved as an individual JSON
    append_target_path = f"energy/backfill/{args['lookback_date']}/backfilled_records.json"
    curr_run_target_path = f"energy/backfill/{args['lookback_date']}/{start_time}-{end_time}_records.json"
    temp_target_path = f"energy/backfill/{args['lookback_date']}/temp_uploaded_blob.json"

    # load each run's data as a unique file
    json_to_gcs(curr_run_target_path, records, bucket)
    if first_run:
        # load the initial run that will be appended
        json_to_gcs(append_target_path, records, bucket)
    else:
        # load the current run's data to gcs as a temp blob to be appended next
        json_to_gcs(temp_target_path, records, bucket)

        # append temp_uploaded_blob.json to a growing json of all data in backfill. As of 11/2021 GCP will not combine
        # more than 32 files, so this operation is performed inside the loop. If fewer files were created overall,
        # this operation could be moved outside the loop.
        bucket_obj = storage_client.bucket(bucket)
        output_file_blob = bucket_obj.blob(append_target_path)
        output_file_blob.content_type = 'application/ndjson'
        output_file_blob.compose([bucket_obj.get_blob(append_target_path), bucket_obj.get_blob(temp_target_path)])

    start_time = end_time