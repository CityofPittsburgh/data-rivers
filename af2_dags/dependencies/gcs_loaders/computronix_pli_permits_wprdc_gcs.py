import os
import argparse


from gcs_utils import json_to_gcs, call_odata_api


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

all_permits = []
all_shadow_job_expansions = []
for (b, i) in zip(bases, fds_id):
    odata_url = F"{url}{b}?{odata_url_date_filter}&{odata_url_base_fields}, {i}, {odata_url_tail}"
    permits = call_odata_api(odata_url)
    for p in permits:
        p.update({"permit_type": b.split("PERMIT")[0]})
    if b == bases[0]:
        for p in permits:
            p.update({"EXTERNALFILENUM": p["PERMITNUMBER"]})
            p.pop("PERMITNUMBER")

    all_permits.extend(permits)


    shadow_url = F"{url}{b}?$expand=SHADOWJOB"
    shadow_jobs = call_odata_api(shadow_url, full_results = False)
    all_shadow_job_expansions.extend(shadow_jobs)


odata_url = F"{url}GENERALPERMIT?{odata_url_date_filter}&{odata_url_base_fields}, PERMITTYPEPERMITTYPE, EXTERNALFILENUM, {odata_url_tail}"
gen_permits = call_odata_api(odata_url)
for g in gen_permits:
    g.update({"permit_type": g["PERMITTYPEPERMITTYPE"]})
    g.pop("PERMITTYPEPERMITTYPE")

all_permits.extend(gen_permits)


# load data into GCS
# out loc = <dataset>/<full date>/<run_id>_all_permits.json
json_to_gcs(args["out_loc"], all_permits, bucket)





# unnested expansion (only on base table)
unnested_table_1 = "SHADOWJOB"
fds_unt1 = 'SNP_NEIGHBORHOOD, SNP_WARD'