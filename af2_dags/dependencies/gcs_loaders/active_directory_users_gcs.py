import os
import ldap


conn = ldap.initialize("ldap://DC1.city.pittsburgh.pa.us")
conn.set_option(ldap.OPT_REFERRALS, 0)
conn.simple_bind_s(rf"{os.environ['WINDOWS_VM_DOMAIN']}\{os.environ['WINDOWS_VM_UN']}", os.environ['WINDOWS_VM_PW'])

result = conn.search_s(f"dc={os.environ['WINDOWS_VM_DOMAIN']},dc=com,ou=Get-ADUser", ldap.SCOPE_SUBTREE,
                       "(mail=*)", ['EmployeeID', 'GivenName', 'Surname', 'Mail', 'Enabled', 'Title',
                                    'Description', 'SamAccountName', 'LastLogonDate'])
print(result)
