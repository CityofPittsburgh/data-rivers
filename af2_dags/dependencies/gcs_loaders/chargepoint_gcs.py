import os
import argparse
import pendulum
from datetime import datetime, timedelta
import requests
import xmltodict
from google.cloud import storage

from gcs_utils import json_to_gcs, find_last_successful_run

# the first recorded charging session was on 10/10/2017
DEFAULT_RUN_START = "2017-10-10T00:00:00Z"
# the API returns 100 records at a time, this allows us to iterate through all records
INCREMENT_RECORDS = 100

storage_client = storage.Client()
bucket = f"{os.environ['GCS_PREFIX']}_chargepoint"

parser = argparse.ArgumentParser()
parser.add_argument('--output_arg', dest='out_loc', required=True,
                    help='fully specified location to upload the combined ndjson file')
args = vars(parser.parse_args())

today = datetime.now(tz = pendulum.timezone("utc")).strftime("%Y-%m-%d")

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

headers = {'Content-Type': 'application/soap+xml'}
interval = 1
# these variables allow us to parse the record information from the XML text returned by the API
start = '</responseText>'
end = '</ns1:getChargingSessionDataResponse>'

# find the last successful DAG run (needs to be specified in UTC YYYY-MM-DD HH:MM:SS) if there was no previous good
# run, default to the date of the first charging session (allows for complete backfill).
# This is used to initialize the payload below
run_start_win, first_run = find_last_successful_run(bucket, "energy/successful_run_log/log.json", DEFAULT_RUN_START)

all_records = []
more = True
while more is True:
    # API call to get data
    response = requests.post(BASE_URL, data=generate_xml(run_start_win, interval), headers=headers)
    vals = response.text[response.text.find(start) + len(start):response.text.rfind(end)]
    vals = '<root>' + vals + '</root>'
    xml_dict = xmltodict.parse(xml_input=vals, encoding='utf-8')
    # continue looping through records until the API has a MoreFlag value of 0
    if xml_dict['root']['MoreFlag']:
        more = (xml_dict['root']['MoreFlag'] == '1')
    else:
        more = False
    records = xml_dict['root']['ChargingSessionData']
    # append list of API results to growing all_records list (assuming API pull contains >100 records)
    all_records += records
    # increment interval by constant value (100) until all records are returned
    interval += INCREMENT_RECORDS

# write the successful run information (used by each successive run to find the backfill start date)
successful_run = {
    "requests_retrieved": len(records),
    "since": run_start_win,
    "current_run": today,
    "note": "Data retrieved between the time points listed above"
}
json_to_gcs("energy/successful_run_log/log.json", [successful_run],
            bucket)

json_to_gcs(f"{args['out_loc']}", all_records, bucket)