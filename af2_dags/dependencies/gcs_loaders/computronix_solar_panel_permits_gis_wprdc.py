import os
import argparse

from gcs_utils import json_to_gcs, call_odata_api_error_handling, \
    write_partial_api_request_results_for_inspection

"""
The permits for solar panels are ingested here. 
These data will be used by GIS for mapping a product that fire (etc.) will use. They will also be published to WPRDC
"""

# parser = argparse.ArgumentParser()
# parser.add_argument('--output_arg', dest = 'out_loc', required = True,
#                     help = 'fully specified location to upload the ndjson file')
# args = vars(parser.parse_args())

bucket = f"{os.environ['GCS_PREFIX']}_computronix"

# CX ODATA API URL base
url = 'https://staff.onestoppgh.pittsburghpa.gov/pghprod/odata/odata/'

# PLI permit tables
base = 'ELECTRICALPERMIT'

# nested expansion tables (expansion from each base table)
xref_1 = "PERMITXSCOPEXREF"
nested_table_1 = "EQUIPMENTTYPE"
xref_2 = "JOBPARCELXREF"
nested_table_2 = "PARCEL"

# fields to select from each table (nt = nested table; unt = unnested table)
fds_base = 'JOBID, EXTERNALFILENUM,STATUS, ISSUEDDATE, COMPLETEDDATE, STATUSDESCRIPTION, COMMERCIALORRESIDENTIAL'
fds_nt2 = 'PARCELNUMBER, ADDRESSOBJECTDEFFORMATTEDADDRESS'

# filter to reduce records to only those for solar panels
odata_record_filter = F"$filter=DESCRIPTION like %SOLAR%"


# build url
odata_url = F"{url}{base}?$select={fds_base}" \ 
F"&$expand={xref_1};" \
F"($expand={nested_table_1}(${odata_record_filter}))" \
F"&$expand={xref_2};" \
F"($expand={nested_table_2}($select{fds_nt2}))"

# call the API
pipe_name = F"{os.environ['GCLOUD_PROJECT']} computronix pli solar panel permits"
permits, error_flag = call_odata_api_error_handling(targ_url = odata_url, pipeline = pipe_name, ct_url = None)


# load data into GCS
# out loc = <dataset>/<full date>/<run_id>_solar_panel_permits.json
if not error_flag:
    json_to_gcs(args["out_loc"], permits, bucket)
else:
    write_partial_api_request_results_for_inspection(permits, "pli_solar_panel_permits")
