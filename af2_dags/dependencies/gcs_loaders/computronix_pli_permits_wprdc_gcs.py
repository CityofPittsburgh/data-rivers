import os
import argparse

from gcs_utils import json_to_gcs, call_odata_api_error_handling, \
    write_partial_api_request_results_for_inspection

"""
There are four types of PLI permits (building, electrical, mechanical, and general) that are ingested here. 
They have to be taken seperately because they're stored in 4 different tables. Each permit is expanded with several 
other tables. There is a table called "SHADOWJOB" that needs to be joined to these permits. An error in the CX system 
prevents navigation to this table. This data is taken in a seperate and will be joined later. SHADOWJOB is a core 
table and can be used for other purposes, so it is ideal to place this process in a second pipeline 
"""

parser = argparse.ArgumentParser()
parser.add_argument('--output_arg', dest = 'out_loc', required = True,
                    help = 'fully specified location to upload the ndjson file')
args = vars(parser.parse_args())

bucket = f"{os.environ['GCS_PREFIX']}_computronix"

# CX ODATA API URL base
url = 'https://staff.onestoppgh.pittsburghpa.gov/pghprod/odata/odata/'

# PLI permit tables
bases = ['BUILDINGPERMIT', 'ELECTRICALPERMIT', 'MECHANICALPERMIT']

# nested expansion tables (expansion from each base table)
xref_1 = "JOBPARCELXREF"
nested_table_1 = "PARCEL"
xref_2 = "PARCELPARCELOWNERXREF"
nested_table_2 = "PARCELOWNER"

# fields to select from each table (nt = nested table; unt = unnested table) (there are differently named ID fields
# for each permit type [ID fields must appear in the same order as the base table])
fds_base = 'ISSUEDATE, ALLCONTRACTORSNAME, TYPEOFWORKDESCRIPTION, COMMERCIALORRESIDENTIAL, WORKDESCRIPTION, ' \
           'TOTALPROJECTVALUE'
fds_id = ["PERMITNUMBER", "EXTERNALFILENUM", "EXTERNALFILENUM"]
fds_nt1 = 'FORMATTEDPARCELNUMBER, ADDRESSABLEOBJEFORMATTEDADDRES'
fds_nt2 = 'OWNERNAME'

# construct query components
odata_url_date_filter = F"$filter=ISSUEDATE gt 2019-06-01T00:00:00Z"
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

# request permits; query will be constructed multiple times for each permit type ('bases' list above) with the
# associated base table's id field ('fds_id' list above)
all_permits = []
for (b, i) in zip(bases, fds_id):
    odata_url = F"{url}{b}?{odata_url_date_filter}&{odata_url_base_fields}, {i}, {odata_url_tail}"

    # basic url to count the total number of records in the outermost entity (useful for logging if the expected number
    # of results were ultimately returned)
    expect_ct_url = F"{url}{b}/$count"
    pipe_name = F"{os.environ['GCLOUD_PROJECT']} computronix pli {b} permits"
    permits, error_flag = call_odata_api_error_handling(targ_url = odata_url, pipeline = pipe_name,
                                                        ct_url = None)
    for p in permits:
        p.update({"permit_type": b.split("PERMIT")[0]})
    if b == bases[0]:
        for p in permits:
            p.update({"EXTERNALFILENUM": p["PERMITNUMBER"]})
            p.pop("PERMITNUMBER")

    all_permits.extend(permits)

# since general permits have a slightly different set of fields, construct the url seperately here
odata_url = F"{url}GENERALPERMIT?{odata_url_date_filter}&{odata_url_base_fields}, PERMITTYPEPERMITTYPE, " \
            F"EXTERNALFILENUM, {odata_url_tail}"

expect_ct_url = F"{url}GENERALPERMIT/$count"
pipe_name = F"{os.environ['GCLOUD_PROJECT']} computronix pli general permits"
gen_permits, error_flag = call_odata_api_error_handling(targ_url = odata_url, pipeline = pipe_name,
                                                        ct_url = None)


# change field to 'permit_type' for consistency with other tables
for g in gen_permits:
    g.update({"permit_type": g["PERMITTYPEPERMITTYPE"]})
    g.pop("PERMITTYPEPERMITTYPE")

# place all permits in a single list
all_permits.extend(gen_permits)

# load data into GCS
# out loc = <dataset>/<full date>/<run_id>_all_permits.json
if not error_flag:
    json_to_gcs(args["out_loc"], all_permits, bucket)
else:
    write_partial_api_request_results_for_inspection(all_permits, "pli_permits")
