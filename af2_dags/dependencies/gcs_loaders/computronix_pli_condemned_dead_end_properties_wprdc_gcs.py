import os
import argparse

from gcs_utils import json_to_gcs, call_odata_api_error_handling, write_partial_api_request_results_for_inspection


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
xref_1 = "PARCELPARCELOWNERXREF"
nested_table_2 = "PARCELOWNER"


# fields to select from each table (nt = nested table)
# for each permit type [ID fields must appear in the same order as the base table])
fds_base = "LATESTINSPECTIONRESULT, LATESTINSPECTIONSCORE, CREATEDDATE, PROGINSPTYPEDESCRIPTION, " \
           "PROGRAMINSPECTIONSTATUS"
fds_nt1 = "PARCELNUMBER, ADDRESSABLEOBJEFORMATTEDADDRES"
fds_nt2 = "OWNERNAME"


# build the URL
odata_url_base_fields = F"$select={fds_base}"
odata_url_tail = F"&$expand={nested_table_1}" \
    F"($select={fds_nt1},; $expand={xref_1}" \
    F"($expand={nested_table_2}($select={fds_nt2})))"
odata_url = F"{url}{base}?&{odata_url_base_fields}{odata_url_tail}"

# basic url to count the total number of records in the outermost entity (useful for logging if the expected number
# of results were ultimately returned) 
expec_ct_url = F"{url}{base}/$count"

# hit the api
pipe_name = F"{os.environ['GCLOUD_PROJECT']}  computronix condemned and dead end properties"
properties, error_flag = call_odata_api_error_handling(targ_url = odata_url, pipeline = pipe_name,
                                                       ct_url = expec_ct_url)

# load data into GCS
# out loc = <dataset>/<full date>/<run_id>_properties.json
if not error_flag:
    json_to_gcs(args["out_loc"], properties, bucket)
else:
    write_partial_api_request_results_for_inspection(properties, "condemened_dead_end_properties")
