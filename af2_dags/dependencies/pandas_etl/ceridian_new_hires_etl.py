from __future__ import absolute_import

import argparse
import io
import os

from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.runtime.client_request_exception import ClientRequestException
from pandas_utils import gcs_to_df

bucket = f"{os.environ['GCS_PREFIX']}_ceridian"
BASE_URL = 'https://cityofpittsburgh.sharepoint.com/'

parser = argparse.ArgumentParser()
parser.add_argument('--gcs_input', dest='gcs_input', required=True,
                    help='fully specified location of source file')
parser.add_argument('--sharepoint_subdir', dest='sharepoint_subdir', required=True,
                    help='path to Sharepoint subdirectory where output file will be stored')
parser.add_argument('--sharepoint_output', dest='sharepoint_output', required=True,
                    help='name of output file that will be stored in Sharepoin')
args = vars(parser.parse_args())


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

site_name = 'sites/IandP/'
url_shrpt = BASE_URL + site_name
auth_ctx = sharepoint_auth(url_shrpt, os.environ['OFFICE365_UN'], os.environ['OFFICE365_PW'])
year = args['sharepoint_subdir'].split('/')[0]
month = args['sharepoint_subdir'].split('/')[1]

upload_to_sharepoint(auth_ctx, df, f"{os.environ['SHAREPOINT_URL']}{year}", args['sharepoint_output'], month)
