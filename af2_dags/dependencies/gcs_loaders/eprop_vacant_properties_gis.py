import os
import argparse
import json
import requests
import re

import pandas as pd

from gcs_utils import json_to_gcs, gen_schema_from_df

# API_LIMIT controls pagination of API request. As of May 2023 it seems that the request cannot be limited to
# selected fields. The unwanted fields are removed later and the required fields are specified here in the namees
# returned directly from the API
API_LIMIT = 10000
FIELDS = ["id", "parcelNumber", "propertyAddress1", "currentOwners", "parcelSquareFootage", "acquisitionMethod",
          "acquisitionDate", "propertyClass", "censusTract", "latitude", "longitude",
          "inventoryType", "zonedAs", "currentStatus", "statusDate"]

bucket = f"{os.environ['GCS_PREFIX']}_eproperty"

# parser = argparse.ArgumentParser()
# parser.add_argument('--output_arg', dest = 'out_loc', required = True,
#                     help = 'fully specified location to upload the processed file')
# args = vars(parser.parse_args())

# Build the API request components
URL_BASE = F"https://api.epropertyplus.com/landmgmt/api/property/"
URL_QUERY = F"/summary?"
url = URL_BASE + URL_QUERY

header = {"Host"            : "api.epropertyplus.com",
          "Connection"      : "keep-alive",
          "x-strllc-authkey": F"{os.environ['EPROPERTY_TOKEN']}",
          "User-Agent"      : "Mozilla/5.0(Macintosh;IntelMacOSX10_9_2...",
          "Content-Type"    : "application/json",
          "Accept"          : "*/*",
          "Accept-Encoding" : "gzip, deflate, sdch",
          "Accept-Language" : "en - US, en;q = 0.8"}

params = {"page": 1, "limit": API_LIMIT}

# Hit the API
all_records = []
more = True
while more is True:
    # API call to get data
    response = requests.get(url, headers = header, params = params)

    # if call is successful and there are records returned then append them to the growing list. if there are no more
    # records in the system, the API will still return 200 codes. "rows" will be an empty list if all the records are
    # retrieved. break out of the loop once this happens.
    if response.status_code == 200:
        new_records = json.loads(response.content.decode('utf-8'))["rows"]
        if new_records:
            all_records = all_records + new_records
            params.update({"page": (params["page"] + 1)})

        # break out of While Loop...no more records
        else:
            more = False

# place API request results in DataFrame for limited pre processing. Typically this would be done in DataFlow.
# However, the limited processing that occurs here is far faster in pandas and avoids unnecessary operations in the
# DAG. This processing can be converted to a DataFlow pipeline later, if the dataset grows and requires this.
# However, as of May 2023, this does not seem likely to occur
df_records = pd.DataFrame(all_records)

drops = [f for f in df_records.columns.to_list() if f not in FIELDS]
df_records.drop(drops, axis = 1, inplace = True)

# insert an underscore between any camel cased characters, convert to lower case, and remove numbers
# change all column names. reorder column for easy inspection.
name_changes = {}
for old in FIELDS:
    old_conv = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', old)
    new_name = re.sub('([a-z0-9])([A-Z])', r'\1_\2', old_conv).lower().strip()
    new_name = re.sub(r'[0-9]', '', new_name)
    name_changes.update({old: new_name})

df_records.rename(columns = name_changes, inplace = True)
df_records = df_records[name_changes.values()]

# convert id, an int, to string to be consistent with our SOP
df_records["id"] = df_records["id"].astype(str)

# generate avro schema
s = gen_schema("eproperties_vacant_properties", df_records)

# load API results as a json to GCS and use avro to load directly into BQ
j = df_records.to_dict(orient = 'records')
json_to_gcs(f"{args['out_loc']}", j, bucket)