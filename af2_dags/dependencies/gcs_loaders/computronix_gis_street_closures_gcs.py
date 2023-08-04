import os
import argparse

from gcs_utils import json_to_gcs, unnest_domi_street_seg, call_odata_api_error_handling

# names to swap for fields that are not nested in raw data
SWAPS = [
        ["EXTERNALFILENUM", "PERMITTYPEPERMITTYPE", "WORKDESCRIPTION", "TYPEOFWORKDESCRIPTION",
         "APPLICANTCUSTOMFORMATTEDNAME", "ALLCONTRACTORSNAME"],

        ["ext_file_num", "permit_type", "work_desc", "type_work_desc", "applicant_name", "contractor_name"]
]

# names to swap for the fields that are nested
OLD_KEYS = ["SPECIALINSTRUCTIONS", "FROMDATE", "TODATE", "WEEKDAYWORKHOURS", "WEEKENDWORKHOURS",
            "PRIMARYSTREET", "FROMSTREET", "TOSTREET", "FULLCLOSURE", "TRAVELLANE", "PARKINGLANE",
            "METEREDPARKING", "SIDEWALK"]
NEW_KEYS = ["special_instructions", "from_date", "to_date", "weekday_hours", "weekend_hours",
            "primary_street", "from_street", "to_street", "full_closure", "travel_lane", "parking_lane",
            "metered_parking", "sidewalk"]

parser = argparse.ArgumentParser()
parser.add_argument('--output_arg', dest = 'out_loc', required = True,
                    help = 'fully specified location to upload the ndjson file')
args = vars(parser.parse_args())

bucket = f"{os.environ['GCS_PREFIX']}_computronix"

# CX ODATA API URL base
url = 'https://staff.onestoppgh.pittsburghpa.gov/pghprod/odata/odata/'

# fields to select from a each table (inserted into "tables" list in the order the tables appear)
fds_base = "EXTERNALFILENUM,PERMITTYPEPERMITTYPE,WORKDESCRIPTION,TYPEOFWORKDESCRIPTION," \
        "APPLICANTCUSTOMFORMATTEDNAME,ALLCONTRACTORSNAME"

fds_nt1 = "SPECIALINSTRUCTIONS,FROMDATE,TODATE,WEEKDAYWORKHOURS,WEEKENDWORKHOURS,PRIMARYSTREET,FROMSTREET," \
        "TOSTREET,FULLCLOSURE,TRAVELLANE,PARKINGLANE,METEREDPARKING,SIDEWALK"

fds_nt2 = "CARTEID"

# base table and nested tables (nt)
tb_base = "DOMIPERMIT"
tb_nt1 = 'DOMISTREETCLOSURE'
tb_nt2 = "STREETCLOSUREDOMISTREETSEGMENT"

odata_url = F"{url}{tb_base}?$select={fds_base}," + F"&$expand={tb_nt1}" \
            + F"($select={fds_nt1},;" + F"$expand={tb_nt2}($select={fds_nt2})),"

# extract the data from ODATA API

nested_permits = call_odata_api_error_handling(odata_url,
                                               F"{os.environ['GCLOUD_PROJECT']} computronix gis street closures")
unnested_data = unnest_domi_street_seg(nested_permits, SWAPS, OLD_KEYS, NEW_KEYS)

# load data into GCS
# out loc = <dataset>/<full date>/<run_id>_str_closures.json
json_to_gcs(args["out_loc"], unnested_data, bucket)
