import os
import argparse
import requests

from gcs_utils import json_to_gcs


def hit_cx_odata_api(url):
    records = []
    more_links = True
    while more_links:
        res = requests.get(url)
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


# CX ODATA API URL base
url = 'https://staff.onestoppgh.pittsburghpa.gov/pghprod/odata/odata/'

# PLI permit tables
bases = ["CASEFILE"]

# nested expansion tables (expansion from each base table)
            # xref_1 = "Y"
            # nested_table_1 = "Z"

# unnested expansion (only on base table)
unnested_table_1 = "INVESTIGATION"

# fields to select from each table (nt = nested table; unt = unnested table)
fds_base = "FD1"
            # fds_nt1 = "FD2"
fds_unt1 = "FD3"

odata_url_date_filter = F"$filter=ISSUEDATE gt 2020-06-01T00:00:00Z"

odata_url_base_fields = F"$select={fds_base}"

odata_url_tail = F"&$expand={xref_1}" \
    "(" \
        F"$select={nested_table_1},; $expand={nested_table_1}" \
            "(" \
                F"$select={fds_nt1},; $expand={xref_2}" \
                    "(" \
                        F"$select={nested_table_2},; $expand={nested_table_2}" \
                            F"($select={fds_nt2})" \
                    ")" \
            ")" \
    ")"

all_permits = []
for (b, i) in zip(bases, fds_id):
    odata_url = F"{url}{b}?{odata_url_date_filter}&{odata_url_base_fields}, {i}, {odata_url_tail}"
    permits = hit_cx_odata_api(odata_url)
    for p in permits:
        p.update({"permit_type": b.split("PERMIT")[0]})
    if b == bases[0]:
        for p in permits:
            p.update({"EXTERNALFILENUM": p["PERMITNUMBER"]})
            p.pop("PERMITNUMBER")

    all_permits.extend(permits)


# load data into GCS
# out loc = <dataset>/<full date>/<run_id>_str_closures.json
json_to_gcs(args["out_loc"], all_violations, bucket)

