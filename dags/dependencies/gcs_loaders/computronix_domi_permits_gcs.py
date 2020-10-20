import requests
import os
import argparse

from gcs_utils import storage_client, json_to_gcs, get_computronix_odata


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
    'DOMISTREETCLOSURE'
    'COMMERCIALORRESIDENTIAL',
    'COMPLETEDDATE',
    'FROMDATE',
    'TODATE',
    'FROMSTREET',
    'TOSTREET',
    'FULLCLOSURE',
    'PARKINGLANE',
    'METEREDPARKING',
    'TRAVELLANE',
    'SIDEWALK',
    'WEEKDAYWORKHOURS',
    'WEEKENDWORKHOURS',
    'DETOUR',
    'ROADCLOSURE',
    'ROADCLOSURESTARTDATE',
    'ROADCLOSUREENDDATE',
    'ROADCUTAREA',
    'ROADCUTDEPTH',
    'ROADCUTLENGTH',
    'ROADCUTWIDTH',
    'ROWOCCUPANCYSTARTDATE',
    'ROWOCCUPANCYENDDATE',
    'ROWOCCUPANCYTYPE',
    'SHOULDERCUTAREA',
    'SHOULDERCUTDEPTH',
    'SHOULDERCUTLENGTH',
    'SHOULDERCUTWIDTH',
    'SIDEWALKLENGTH',
    'SIDEWALKWIDTH',
    'NEWEXISTING',
    'NOPARKINGAUTHORIZATION',
    'NUMBEROFDUMPSTERS',
    'NUMBEROFMACHINES',
    'SPECIALPERMITINSTRUCTIONS',
    'PRIMARYSTREET',
    'APPLICANTCUSTOMEROBJECTID',
    'STATUSDESCRIPTION',
    'STREETTREEWITHINAREAOFWORK',
    'EFFECTIVEDATE',
    'EXPIRATIONDATE'
]

EXPAND_FIELDS = [
    'ADDRESS',
    'LOCATION',
    'PERMITTYPE',
    'PROJECT',
    'TYPEOFWORK',
    'DOMIPERMITCURBCUTXREF',
    'DOMIPERMITPOLEXREF',
    'DOMIPERMITTREEXREF',
    'DOMISTREETCLOSURE',
    'PERMITWORKSCOPEXREF',
    'JOBPARCELXREF'
]

domi_permits = get_computronix_odata('DOMIPERMIT', expand_fields=EXPAND_FIELDS)

json_to_gcs('domi_permits/{}/{}/{}_domi_permits.json'.format(args['execution_date'].split('-')[0],
                                                             args['execution_date'].split('-')[1],
                                                             args['execution_date']),
            domi_permits, bucket)
