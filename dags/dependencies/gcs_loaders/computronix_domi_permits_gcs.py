import requests
import os
import argparse

from gcs_utils import storage_client, json_to_gcs, get_computronix_odata, filter_fields


parser = argparse.ArgumentParser()
parser.add_argument('-e', '--execution_date', dest='execution_date',
                    required=True,
                    help='DAG execution date (YYYY-MM-DD)')
args = vars(parser.parse_args())

bucket = '{}_computronix'.format(os.environ['GCS_PREFIX'])

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

domi_permits = get_computronix_odata('DOMIPERMIT', expand_fields=EXPAND_FIELDS)
trimmed_permits = filter_fields(domi_permits, RELEVANT_FIELDS)

json_to_gcs('domi_permits/{}/{}/{}_domi_permits.json'.format(args['execution_date'].split('-')[0],
                                                             args['execution_date'].split('-')[1],
                                                             args['execution_date']),
            trimmed_permits, bucket)
