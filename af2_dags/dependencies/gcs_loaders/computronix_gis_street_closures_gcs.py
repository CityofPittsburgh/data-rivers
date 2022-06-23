import os
import argparse

from gcs_utils import json_to_gcs, select_expand_odata, unnest_domi_street_seg

# names to swap for fields that are not nested in raw data
SWAPS = [
        ["EXTERNALFILENUM", "PERMITTYPEPERMITTYPE", "WORKDESCRIPTION", "TYPEOFWORKDESCRIPTION",
         "APPLICANTCUSTOMFORMATTEDNAME", "ALLCONTRACTORSNAME", "CREATEDDATE"],

        ["ext_file_num", "permit_type", "work_desc", "type_work_desc", "applicant_name", "contractor_name",
         "create_date"]
]

# names to swap for the fields that are nested
OLD_KEYS = ["SPECIALINSTRUCTIONS", "FROMDATE", "TODATE", "WEEKDAYWORKHOURS", "WEEKENDWORKHOURS",
            "PRIMARYSTREET", "FROMSTREET", "TOSTREET", "FULLCLOSURE", "TRAVELLANE", "PARKINGLANE",
            "METEREDPARKING", "SIDEWALK", "VALIDATED"]
NEW_KEYS = ["special_instructions", "from_date", "to_date", "weekday_hours", "weekend_hours",
            "primary_street", "from_street", "to_street", "full_closure", "travel_lane", "parking_lane",
            "metered_parking", "sidewalk", "validated"]

parser = argparse.ArgumentParser()
parser.add_argument('--output_arg', dest = 'out_loc', required = True,
                    help = 'fully specified location to upload the ndjson file')
args = vars(parser.parse_args())

bucket = f"{os.environ['GCS_PREFIX']}_computronix"

# CX ODATA API URL base
url = 'https://staff.onestoppgh.pittsburghpa.gov/pghprod/odata/odata/'

# fields to select from a each table (inserted into "tables" list in the order the tables appear)
sel_fields = [
        "EXTERNALFILENUM,PERMITTYPEPERMITTYPE,WORKDESCRIPTION,TYPEOFWORKDESCRIPTION," \
        "APPLICANTCUSTOMFORMATTEDNAME,ALLCONTRACTORSNAME,CREATEDDATE",

        "SPECIALINSTRUCTIONS,FROMDATE,TODATE,WEEKDAYWORKHOURS,WEEKENDWORKHOURS,PRIMARYSTREET,FROMSTREET," \
        "TOSTREET,FULLCLOSURE,TRAVELLANE,PARKINGLANE,METEREDPARKING,SIDEWALK,VALIDATED",

        "UNIQUEID,CARTEID"
]

# first tuple refers to the base table & subsequent tuples are expansions
# "STREETCLOSUREDOMISTREETSEGMENT" is a nested join into 'DOMISTREETCLOSURE'
tables = [
        ("DOMIPERMIT", False, [sel_fields[0]]),
        ('DOMISTREETCLOSURE', ["STREETCLOSUREDOMISTREETSEGMENT"], [sel_fields[1], sel_fields[2]])
]

# extract the data from ODATA API
nested_permits = select_expand_odata(url, tables, limit_results = False)

unnested_data = unnest_domi_street_seg(nested_permits, SWAPS, OLD_KEYS, NEW_KEYS)[0]

# load data into GCS
# out loc = <dataset>/<full date>/<run_id>_str_closures.json
json_to_gcs(args["out_loc"], unnested_data, bucket)
