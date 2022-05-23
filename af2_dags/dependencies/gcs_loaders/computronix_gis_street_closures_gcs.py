import os
import argparse

from gcs_utils import json_to_gcs, select_expand_odata



parser = argparse.ArgumentParser()
parser.add_argument('--output_arg', dest='out_loc', required=True,
                    help='fully specified location to upload the ndjson file')
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
domi_str_close = select_expand_odata(url, tables, limit_results = True)

# load data into GCS
# out loc = <dataset>/<full date>/<run_id>_str_closures.json
json_to_gcs(args["out_loc"], domi_str_close, bucket)

