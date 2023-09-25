import os
import argparse
from ldap3 import Server, Connection, NONE, SUBTREE, DEREF_NEVER
from google.cloud import storage
from gcs_utils import json_to_gcs


def set_domain_name(dept_name):
    return f"OU={dept_name},OU=Users,OU=Next,DC={os.environ['LDAP_URI'].split('.')[1]}," \
           f"DC={os.environ['LDAP_URI'].split('.')[2]},DC={os.environ['LDAP_URI'].split('.')[3]}," \
           f"DC={os.environ['LDAP_URI'].split('.')[4]}"


storage_client = storage.Client()
bucket = f"{os.environ['GCS_PREFIX']}_active_directory"

parser = argparse.ArgumentParser()
parser.add_argument('--output_arg', dest='out_loc', required=True,
                    help='fully specified location to upload the combined ndjson file')
args = vars(parser.parse_args())

# define the server
s = Server(f"ldap://{os.environ['LDAP_URI']}", use_ssl=True, get_info=NONE)

# define the connection
conn = Connection(s, user=rf"{os.environ['WINDOWS_VM_DOMAIN']}\{os.environ['WINDOWS_VM_UN']}",
                  password=os.environ['WINDOWS_VM_PW'])

# perform the Bind operation
if not conn.bind():
    print('Error in bind', conn.result)
else:
    # filter out users with blank emails, test users, and non-user shared accounts (plus one specific test account)
    ldap_filter = '(&(objectClass=user)(mail=*)(!(mail=test*))(!(employeeID=Shared*))(!(sAMAccountName=mousem)))'
    ldap_attributes = ['employeeID', 'givenName', 'sn', 'mail', 'sAMAccountName',
                       'title', 'description', 'userAccountControl']

    # LDAP query responses are limited to 1000 rows at a time - this breaks the responses up by department
    # and allows smaller sets of results that do not break the query
    dept_names = ["BBI", "CHR", "CON", "Controller's Office", "COU",
                  "CPRB", "DCP", "DOMI", "EMA", "EMS", "FIN", "Fire",
                  "I&P", "JTPA", "Law", "Mayor's", "PAR", "Pension",
                  "Personnel", "Police", "PS", "Public Works",  "Recipients",
                  "Resources", "Service Accounts", "Training", "Vendors"]
    all_users = []
    # loop through domain objects
    for dept in dept_names:
        ldap_dn = set_domain_name(dept)
        conn.search(ldap_dn, ldap_filter, SUBTREE, DEREF_NEVER, ldap_attributes)

        placeholder = None
        for entry in conn.response:
            # User attributes are returned as CaseInsensitiveDicts, which means the values are nested within lists
            # even if there is only a single value. This code unnests the attribute data
            val = {key: (value[0] if value else None) for key, value in entry['attributes'].items()}
            all_users.append(val)

    conn.unbind()
    json_to_gcs(f"{args['out_loc']}", all_users, bucket)
