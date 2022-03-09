import os
import argparse

from gcs_utils import json_to_gcs, get_computronix_odata, filter_fields

bucket = f"gs://{os.environ['GCS_PREFIX']}_computronix"

parser = argparse.ArgumentParser()
parser.add_argument('--output_arg', dest='out_loc', required=True,
                    help='fully specified location to upload the ndjson file')
args = vars(parser.parse_args())


RELEVANT_FIELDS = [
    'EXTERNALFILENUM',
    'JOBID',
    'PARENTJOBID',
    'PERMITTYPEPERMITTYPE',
    'WORKDESCRIPTION',
    'TYPEOFWORKDESCRIPTION',
    'APPLICANTCUSTOMFORMATTEDNAME',
    'ALLCONTRACTORSNAME',
    'ADDRESS',
    'LOCATION',
    'DOMISTREETCLOSURE',
    'COMMERCIALORRESIDENTIAL',
    'COMPLETEDDATE',
    'NOPARKINGAUTHORIZATION',
    'DETOUR',
    'NUMBEROFDUMPSTERS',
    'NUMBEROFMACHINES',
    'SPECIALPERMITINSTRUCTIONS',
    'APPLICANTCUSTOMEROBJECTID',
    'STATUSDESCRIPTION',
    'EFFECTIVEDATE',
    'EXPIRATIONDATE',
    'WORKDATESFROM',
    'WORKDATESTO'
]

EXPAND_FIELDS = [
    'ADDRESS',
    'LOCATION',
    'PERMITTYPE',
    'PROJECT',
    'DOMISTREETCLOSURE'
]


#TODO: add in a look back to prev run and only pull new since then-> CX doesn't seem to keep records based on time.
# The API seems to return all records (ever) in pulls of 200 (over and over). If we can determine we already have X
# records we can only pull from X+1 perhaps


domi_permits = get_computronix_odata('DOMIPERMIT', expand_fields=EXPAND_FIELDS)
trimmed_permits = filter_fields(domi_permits, RELEVANT_FIELDS)

json_to_gcs(args["out_loc"],trimmed_permits, bucket)
