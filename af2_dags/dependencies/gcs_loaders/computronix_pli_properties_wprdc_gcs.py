import os
import argparse
import requests

from gcs_utils import json_to_gcs


def hit_cx_odata_api(odata_url):
    records = []
    more_links = True
    while more_links:
        res = requests.get(odata_url)
        records.extend(res.json()['value'])

        if res.status_code != 200:
            print("API call failed")
            print(f"Status Code:  {res.status_code}")

        if '@odata.nextLink' in res.json().keys():
            odata_url = res.json()['@odata.nextLink']
        else:
            more_links = False

    return records


parser = argparse.ArgumentParser()
parser.add_argument('--output_arg', dest = 'out_loc', required = True,
                    help = 'fully specified location to upload the ndjson file')
args = vars(parser.parse_args())

bucket = f"{os.environ['GCS_PREFIX']}_computronix"


# CX ODATA API URL base
url = 'https://staff.onestoppgh.pittsburghpa.gov/pghprod/odata/odata/'

# PLI base and nested expansion tables (expansion from base table)
base = "CE_PROGRAMINSPECTIONLOCATION"
nested_table_1 = "PARCEL"
nested_table_2 = "PARCELPARCELOWNERXREF"
nested_table_3 = "PARCELOWNER"


# fields to select from each table (nt = nested table)
# for each permit type [ID fields must appear in the same order as the base table])
fds_base = "LATESTINSPECTIONRESULT, LATESTINSPECTIONSCORE, CREATEDDATE, PROGINSPTYPEDESCRIPTION, " \
           "PROGRAMINSPECTIONSTATUS"
fds_nt1 = "PARCELNUMBER, ADDRESSABLEOBJEFORMATTEDADDRES"
fds_nt2 = "PARCELOWNEROBJECTID, PARCELOBJECTID"
fds_nt3 = "OWNERNAME"


# build the URL
odata_url_base_fields = F"$select={fds_base}"
odata_url_tail = F"&$expand={nested_table_1}" \
    "(" \
        F"$select={fds_nt1},; $expand={nested_table_2}" \
            "(" \
                F"$expand={nested_table_3}($select={fds_nt3})" \
            ")" \
    ")"
odata_url = F"{url}{base}?&{odata_url_base_fields}{odata_url_tail}"


# hit the api
permits = hit_cx_odata_api(odata_url)


# load data into GCS
# out loc = <dataset>/<full date>/<run_id>_str_closures.json
json_to_gcs(args["out_loc"], permits, bucket)

