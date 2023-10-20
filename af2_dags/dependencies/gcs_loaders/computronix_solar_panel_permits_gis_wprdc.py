import os
import argparse

import pandas as pd


from gcs_utils import json_to_gcs, call_odata_api_error_handling, \
    write_partial_api_request_results_for_inspection

"""
The permits for solar panels are ingested here. 
These data will be used by GIS for mapping a product that fire (etc.) will use. They will also be published to WPRDC
"""

parser = argparse.ArgumentParser()
parser.add_argument('--output_arg', dest = 'out_loc', required = True,
                    help = 'fully specified location to upload the ndjson file')
args = vars(parser.parse_args())

bucket = f"{os.environ['GCS_PREFIX']}_computronix"

# CX ODATA API URL base
url = 'https://staff.onestoppgh.pittsburghpa.gov/pghprod/odata/odata/'

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
odata_url_base = F"{url}{tables['bt']}?"
odata_url_left = F"&$select={fds['bt']}" \
                 F"&$expand={tables['left_xref']}" \
                 F"(" \
                 F"$select={fds['left_xref']},;" \
                 F"$expand={tables['left_nt']}" \
                 F"($select={fds['left_nt']})" \
                 "),"

odata_url_right = F"{tables['right_xref']}" \
                  F"(" \
                  F"$select={fds['right_xref']},;" \
                  F"$expand={tables['right_nt']}" \
                  F"($select={fds['right_nt']})" \
                  F")"

odata_url = odata_url_base + odata_url_left + odata_url_right

# call the API
pipe_name = F"{os.environ['GCLOUD_PROJECT']} computronix pli solar panel permits"
permits, error_flag = call_odata_api_error_handling(targ_url = odata_url, pipeline = pipe_name, ct_url = None)

# expand the nested PERMITWORKSCOPEXREF column (there can be more than 1 entry here, though no other fields can have
# more than 1 entry) (this is significantly more concise than a dataflow operation to accomplish this unnesting. this
# type of transformation could be done in dataflow in the future, and a BEAM embedded dataframe may be implemented in
# place of this.
permit_unnest = pd.DataFrame(permits).explode("PERMITWORKSCOPEXREF")
permit_unnest["work_desc"] = pd.json_normalize(permit_unnest["PERMITWORKSCOPEXREF"])["WORKSCOPE.DESCRIPTION"]


# load data into GCS
# out loc = <dataset>/<full date>/<run_id>_solar_panel_permits.json
if not error_flag:
    json_to_gcs(args["out_loc"], permits, bucket)
else:
    write_partial_api_request_results_for_inspection(permits, "pli_solar_panel_permits")
