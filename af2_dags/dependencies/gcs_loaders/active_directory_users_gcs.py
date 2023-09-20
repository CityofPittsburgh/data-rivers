import os
import argparse
import ldap
from google.cloud import storage
from gcs_utils import json_to_gcs


storage_client = storage.Client()
bucket = f"{os.environ['GCS_PREFIX']}_active_directory"

parser = argparse.ArgumentParser()
parser.add_argument('--output_arg', dest='out_loc', required=True,
                    help='fully specified location to upload the combined ndjson file')
args = vars(parser.parse_args())

conn = ldap.initialize(f"ldap://{os.environ['LDAP_URI']}")
conn.simple_bind_s(rf"{os.environ['WINDOWS_VM_DOMAIN']}\{os.environ['WINDOWS_VM_UN']}", os.environ['WINDOWS_VM_PW'])


def set_domain_name(dept_name):
    return f"OU={dept_name},OU=Users,OU=Next,DC={os.environ['LDAP_URI'].split('.')[1]}," \
           f"DC={os.environ['LDAP_URI'].split('.')[2]},DC={os.environ['LDAP_URI'].split('.')[3]}," \
           f"DC={os.environ['LDAP_URI'].split('.')[4]}"


ldap_filter = '(&(objectClass=user)(mail=*))'
ldap_attributes = ['employeeID', 'givenName', 'sn', 'mail', 'sAMAccountName',
                   'title', 'description', 'userAccountControl']

dept_names = ["BBI", "CHR", "CON", "Controller's Office", "COU",
              "CPRB", "DCP", "DOMI", "EMA", "EMS", "FIN", "Fire",
              "I&P", "JTPA", "Law", "Mayor's", "PAR", "Pension",
              "Personnel", "Police", "PS", "Public Works",  "Recipients",
              "Resources", "Service Accounts", "Training", "Vendors"]
all_users = []
for dept in dept_names:
    ldap_dn = set_domain_name(dept)
    result = conn.search_s(ldap_dn, ldap.SCOPE_SUBTREE, ldap_filter, ldap_attributes)

    for dn, entry in result:
        user_dict = {}
        for attr in ldap_attributes:
            user_dict[attr] = entry.get(attr, [None])[0].decode('utf-8') if entry.get(attr) else None
        all_users.append(user_dict)

conn.unbind()
json_to_gcs(f"{args['out_loc']}", all_users, bucket)
