import os
import argparse

import pandas as pd

from gcs_utils import call_odata_api_error_handling, conv_avsc_to_bq_schema

"""
The permits for solar panels are ingested here. 
These data will be used by GIS for mapping a product that fire (etc.) will use. They will also be published to WPRDC
"""

# CX ODATA API URL base
URL = 'https://staff.onestoppgh.pittsburghpa.gov/pghprod/odata/odata/'

# rename columns
NEW_NAMES = {"JOBID"                  : "job_id",
             "STATUSDESCRIPTION": "status",
             "COMMERCIALORRESIDENTIAL": "commercial_residential",
             "COMPLETEDDATE"          : "completed_date",
             "ISSUEDATE"              : "issue_date",
             "EXTERNALFILENUM": "ext_file_num",
             "PERMITWORKSCOPEXREF": "work_scope"
             }

parser = argparse.ArgumentParser()
parser.add_argument('--output_arg', dest = 'out_loc', required = True,
                    help = 'fully specified location to upload the ndjson file')
args = vars(parser.parse_args())

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
pipe_name = F"{os.environ['GCLOUD_PROJECT']} computronix pli solar panel permits"
permits, error_flag = call_odata_api_error_handling(targ_url = odata_url, pipeline = pipe_name, ct_url = None)


# filter the unnecessary records. the table needed to filter these records isn't accessible through this api
# currently (10/23) and the filtering must be done here
permit_df = pd.DataFrame(permits)
strs = permit_df["PERMITWORKSCOPEXREF"].astype(str).to_list()
strs_clean = [s.lower().__contains__("solar") for s in strs]
solar_permits = permit_df[strs_clean].copy()

solar_permits.rename(columns = NEW_NAMES, inplace = True)

# extract nest address and parc number. list comprehension was tested to be the fastest method 10/23 given the
# current size of data and it's projected size for several years to come.
info = pd.json_normalize(solar_permits["JOBPARCELXREF"])[0].to_list()
parc_num = ["None" if i is None else i["PARCEL.PARCELNUMBER"] for i in info]
address = ["None" if i is None else i['PARCEL.ADDRESSABLEOBJEFORMATTEDADDRES'] for i in info]
solar_permits["parc_num"] = parc_num
solar_permits["address"] = address
solar_permits.drop("JOBPARCELXREF", axis = 1, inplace = True)

solar_permits["job_id"] = solar_permits["job_id"].astype(str)

# load into BQ via the avsc file in schemas direc
schema = conv_avsc_to_bq_schema(F"{os.environ['GCS_PREFIX']}_avro_schemas", "computronix_solar_permits.avsc")
solar_permits.to_gbq("computronix.solar_permits", project_id = f"{os.environ['GCLOUD_PROJECT']}",
                     if_exists = "replace", table_schema = schema)
