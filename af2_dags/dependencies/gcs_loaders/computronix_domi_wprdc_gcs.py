import os
import argparse

from gcs_utils import json_to_gcs, select_expand_odata


parser = argparse.ArgumentParser()
parser.add_argument('--output_arg', dest = 'out_loc', required = True,
                    help = 'fully specified location to upload the ndjson file')
args = vars(parser.parse_args())

bucket = f"{os.environ['GCS_PREFIX']}_computronix"


# CX ODATA API URL base
url = 'https://staff.onestoppgh.pittsburghpa.gov/pghprod/odata/odata/'


bases = ['BUILDINGPERMIT', 'ELECTRICALPERMIT', 'MECHANICALPERMIT']
fds_base = 'ISSUEDATE, ALLCONTRACTORSNAME, TYPEOFWORKDESCRIPTION, COMMERCIALORRESIDENTIAL, WORKDESCRIPTION, ' \
            'TOTALPROJECTVALUE' # PERMITNUMBER,
fds_parcel = 'FORMATTEDPARCELNUMBER, ADDRESSABLEOBJEFORMATTEDADDRES'
fds_shadowjob = 'SNP_NEIGHBORHOOD, SNP_WARD'
fds_owner = 'OWNERNAME'














# should we do this for eac permit type?
# hit the API shoujld be easy with the util
# we will join all 4 hits
# but the "geneeral peremits" table needs a field thaat the other don't have. it'll be a more descriptive suib type
# that "general"
# solution - insert a col called permit_type with generic deescriptors from all tables except for genereal you get
# the real value from PermitTYpePeermitTYpe
#



#
# tables = [
#              (b, False, [fds_base]),
#             ("JOBPARCELXREF", ["PARCEL"], ["PARCEL", fds_parcel]),
#             ("SHADOWJOB", False, [sel_fields[2]]),
#             ("OWNERNAME", False, [sel_fields[3]])
#     ]


# ("JOBPARCELXREF", ["PARCEL"], ["PARCEL", parcel_fds]),


permits = []
for b in bases:
    # first tuple refers to the base table & subsequent tuples are expansions
    tables = [
            (b, False, [fds_base]),
            ("JOBPARCELXREF", ["PARCEL"], ["PARCEL", fds_parcel]),

    ]

            #            ("SHADOWJOB", False, [fds_shadowjob])

# ]







    # extract the data from ODATA API
    permits.append(select_expand_odata(url, tables, limit_results = False))



# load data into GCS
# out loc = <dataset>/<full date>/<run_id>_str_closures.json
json_to_gcs(args["out_loc"], permits, bucket)

