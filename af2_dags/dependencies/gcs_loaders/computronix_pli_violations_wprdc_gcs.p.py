import os
import argparse
import requests
from datetime import date

from gcs_utils import json_to_gcs


def hit_cx_odata_api(targ_url):
    records = []
    more_links = True
    while more_links:
        res = requests.get(targ_url)
        records.extend(res.json()['value'])

        if res.status_code != 200:
            print("API call failed")
            print(f"Status Code:  {res.status_code}")

        if '@odata.nextLink' in res.json().keys():
            url = res.json()['@odata.nextLink']
        else:
            more_links = False

    return records


parser = argparse.ArgumentParser()
parser.add_argument('--output_arg', dest = 'out_loc', required = True,
                    help = 'fully specified location to upload the ndjson file')
args = vars(parser.parse_args())

bucket = f"{os.environ['GCS_PREFIX']}_computronix"

# CX ODATA API URL service root
root = 'https://staff.onestoppgh.pittsburghpa.gov/pghprod/odata/odata/'

# PLI Entity
base = "CASEFILE"

# unnested expansion (only on base table)
unnested_table_1 = "INVESTIGATION"
unnested_table_2 = "VIOLATION"
unnested_table_3 = "CASEFILETYPE"

# fields to select from each table (nt = nested table; unt = unnested table)
fds_base = "EXTERNALFILENUM, STATUSDESCRIPTION, ADDRESSFORMATTEDADDRESS, PARCELPARCELNUMBER"
fds_unt1 = "DATECOMPLETED, OUTCOME, FINDINGS"
fds_unt2 = "CODESECTION, CODESECTIONTITLE, DESCRIPTION, SPECIALINSTRUCTIONS"
fds_unt3 = "NAME"

# build filter which excludes data before implementation of CX system (6/20) and data that have not yet been
# inspected (which are not meaningul at that point. The data will be included when the subsequent inspection occurs)
today = date.today()
date_filter = F"$filter=DATECOMPLETED gt 2020-06-01T00:00:00Z and DATECOMPLETED lt {today}T00:00:00Z"

# build url
odata_url = F"{root}{base}?" \
            F"$select={fds_base}, " \
            "&" \
            F"$expand={unnested_table_1}" \
            F"($select={fds_unt1};{date_filter}), " \
            F"{unnested_table_2}($select={fds_unt2}), " \
            F"{unnested_table_3}($select={fds_unt3})"

# get violations from API
violations = hit_cx_odata_api(odata_url)

# load data into GCS
# out loc = <dataset>/<full date>/<run_id>_violations.json
json_to_gcs(args["out_loc"], violations, bucket)
