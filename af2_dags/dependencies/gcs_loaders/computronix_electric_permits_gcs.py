import os
import argparse

from gcs_utils import json_to_gcs, call_odata_api_error_handling, write_partial_api_request_results_for_inspection

"""
The permits for solar panels are ingested here. 
These data will be used by GIS for mapping a product that fire (etc.) will use. They will also be published to WPRDC. 
All electrical permits are downloaded as it is not feasible (10/23) to filter the non solar panel permits at this 
juncture. Th pli_permits DAG also ingests electrical permits along with several other types. It is more streamlined to 
download them seperately here in practice.
"""

# CX ODATA API URL base
URL = 'https://staff.onestoppgh.pittsburghpa.gov/pghprod/odata/odata/'


parser = argparse.ArgumentParser()
parser.add_argument('--output_arg', dest = 'out_loc', required = True,
                    help = 'fully specified location to upload the ndjson file')
args = vars(parser.parse_args())



make hot bucket
bucket = f"{os.environ['GCS_PREFIX']}_computronix"

# build url
# map of table relationships, with a left and right branch to a base table:
# general desc:    |        LEFT                   |         BASE           |       RIGHT              |
# variable name:   left_nt          left_xref                 bt           right_xref      right_nt
# real table name: WORKSCOPE << PERMITWORKSCOPEXREF << *ELECTRICALPERMIT* >> JOBPARCELXREF >> PARCEL
# >> or << are expansions from the base table or from xref table. direction of arrow indicates the direction of the
# expansion outward from the base table (bt)

# tables
tables = {
        "bt"        : 'ELECTRICALPERMIT',
        "left_xref" : "PERMITWORKSCOPEXREF",
        "left_nt"   : "WORKSCOPE",
        "right_xref": "JOBPARCELXREF",
        "right_nt"  : "PARCEL"
}

fds = {
        "bt"        : 'JOBID, STATUSDESCRIPTION, COMMERCIALORRESIDENTIAL, COMPLETEDDATE, ISSUEDATE, EXTERNALFILENUM',
        "left_xref" : "JOBID",
        "left_nt"   : "DESCRIPTION",
        "right_xref": "JOBID",
        "right_nt"  : 'PARCELNUMBER, ADDRESSABLEOBJEFORMATTEDADDRES'
}

# form the url
query_url_components = {
        "base" : F"{URL}{tables['bt']}?",

        "left" : F"&$select={fds['bt']}"
                 F"&$expand={tables['left_xref']}"
                 F"("
                 F"$select={fds['left_xref']},;"
                 F"$expand={tables['left_nt']}"
                 F"($select={fds['left_nt']})"
                 "),",

        "right": F"{tables['right_xref']}"
                 F"("
                 F"$select={fds['right_xref']},;"
                 F"$expand={tables['right_nt']}"
                 F"($select={fds['right_nt']})"
                 F")",
}
odata_url = query_url_components["base"] + query_url_components["left"] + query_url_components["right"]

# call the API
# basic url to count the total number of records in the outermost entity (useful for logging if the expected number
# of results were ultimately returned)
expec_ct_url = F"{URL}{tables['bt']}/$count"
pipe_name = F"{os.environ['GCLOUD_PROJECT']} computronix pli solar panel permits"
print("executing api calling function call in util file")
permits, error_flag = call_odata_api_error_handling(targ_url = odata_url, pipeline = pipe_name, ct_url = None)
print("completed api calling function")

# load data into GCS
# out loc = <dataset>/<full date>/<run_id>_solar_panel_permits.json
if not error_flag:
    json_to_gcs(args["out_loc"], permits, bucket)
else:
    write_partial_api_request_results_for_inspection(permits, "cx_solar_panel_permits_corrupted")



