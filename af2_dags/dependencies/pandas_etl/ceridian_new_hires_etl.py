from __future__ import absolute_import

import argparse
import io
import os

from google.cloud import storage
from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.runtime.client_request_exception import ClientRequestException
from pandas_utils import gcs_to_df

client = storage.Client()
bucket = f"{os.environ['GCS_PREFIX']}_ceridian"
export_bucket = client.get_bucket(f"{os.environ['GCS_PREFIX']}_iapro")
BASE_URL = 'https://cityofpittsburgh.sharepoint.com/'

parser = argparse.ArgumentParser()
parser.add_argument('--gcs_input', dest='gcs_input', required=True,
                    help='fully specified location of source file')
parser.add_argument('--sharepoint_subdir', dest='sharepoint_subdir', required=True,
                    help='path to Sharepoint subdirectory where output file will be stored')
parser.add_argument('--sharepoint_output', dest='sharepoint_output', required=True,
                    help='name of output file that will be stored in Sharepoin')
args = vars(parser.parse_args())


def extract_middle_initial(full_name):
    parts = full_name.split(", ")

    if len(parts) < 2:
        return None

    try:
        first_middle_parts = parts[1].split(" ")
        # Check if there are at least 2 parts in the first_name part
        if len(first_middle_parts) < 2:
            return None
        middle_initial = first_middle_parts[1]
        # Check if the middle_initial is exactly one character long
        if len(middle_initial) == 1:
            return middle_initial
        else:
            return None
    except IndexError:
        return None


# credit: Xiaohong Wang
def sharepoint_auth(url_shrpt, username, password):
    ctx_auth = AuthenticationContext(url_shrpt)
    if ctx_auth.acquire_token_for_user(username, password):
        ctx = ClientContext(url_shrpt, ctx_auth)
        web = ctx.web
        ctx.load(web)
        ctx.execute_query()
        print('Authenticated into Sharepoint as: ', web.properties['Title'])
        return ctx
    else:
        print("failure: " + ctx_auth.get_last_error())


# adapted from https://plainenglish.io/blog/how-to-upload-files-to-sharepoint-using-python
def upload_to_sharepoint(ctx, data, directory, file_name, subdirectory=None):
    if subdirectory:
        target_folder = ctx.web.get_folder_by_server_relative_url(f"{directory}/{subdirectory}")
    else:
        target_folder = ctx.web.get_folder_by_server_relative_url(directory)
    stream = io.StringIO()
    data.to_csv(stream, index=False)

    # if target subdirectory does not exist, create it and then upload the file
    try:
        target_folder.upload_file(file_name, stream.getvalue().encode()).execute_query()
    except ClientRequestException:
        print(f"Subdirectory {subdirectory} not found. Attempting creation...")
        parent_folder = ctx.web.get_folder_by_server_relative_url(directory)
        target_folder = parent_folder.folders.add(subdirectory)
        ctx.execute_query()
        target_folder.upload_file(file_name, stream.getvalue().encode()).execute_query()
    print(f"Successfully uploaded {len(data.index)} rows of data")
    stream.close()


df = gcs_to_df(bucket, f"{args['gcs_input']}")
df['middle_initial'] = df['display_name'].apply(lambda x: extract_middle_initial(x))
df = df[['employee_num', 'first_name', 'last_name', 'middle_initial', 'display_name', 'sso_login',
         'job_title', 'manager_name', 'dept_desc', 'hire_date', 'account_modified_date',
         'pay_class', 'pay_status', 'employment_status']]

site_name = 'sites/IandP/'
url_shrpt = BASE_URL + site_name
auth_ctx = sharepoint_auth(url_shrpt, os.environ['OFFICE365_UN'], os.environ['OFFICE365_PW'])
year = args['sharepoint_subdir'].split('/')[0]
month = args['sharepoint_subdir'].split('/')[1]

export_bucket.blob('new_hire_report.csv').upload_from_string(df.to_csv(), 'text/csv')

shrpt_df = df.drop(columns=['first_name', 'last_name', 'middle_initial'])
upload_to_sharepoint(auth_ctx, shrpt_df, f"{os.environ['SHAREPOINT_URL']}{year}", args['sharepoint_output'], month)
