import os
import argparse
import json
import requests
import pandas as pd
import re
import numpy as np

from gcs_utils import json_to_gcs, conv_avsc_to_bq_schema

# API_LIMIT controls pagination of API request. As of May 2023 it seems that the request cannot be limited to
# selected fields. The unwanted fields are removed later and the required fields are specified here in the namees
# returned directly from the API. Results are uploaded into long-term storage as a json. This is placed in an
# auto-class bucket, so that data that is not touched (essentially each json) for 30 days is moved to colder storage.
# The script also uploads the recrods from a dataframe into BQ. No AVRO is loaded into GCS for later movement into BQ


def normalize_block_lot(x):
    # convert all block lots to the standard format of dehyphenated 16 chars
    # for each section of the incorrectly formatted block/lot, there may not be the correct length of characters as
    # specified in part_len. these to be zero padded on the left and those section may not be there at all.
    if x is not np.nan:
        parts = x.split("-")
        if len(parts) == 1:
            # print("bad value")
            return None

        part_len = [4, 1, 5, 4, 2]

        p = 0
        out = ""
        while p < len(part_len):
            # don't convert alpha only chars
            if len(parts) >= (p + 1) and parts[p].isalpha():
                out = out + parts[p].rjust(part_len[p], "0")
                p += 1
                continue

            # if the current part is missing make it all zeros
            if len(parts) >= (p+1):
                parts[p] = parts[p].rjust(part_len[p], "0")
            else:
                parts.append("".rjust(part_len[p], "0"))

            # append it all together
            out = out + parts[p]
            p += 1
        return out
    else:
        return np.nan


API_LIMIT = 10000
FIELDS = {"id": "id", "parcelNumber": "parc", "propertyAddress1": "address",
          "currentOwners": "owner", "parcelSquareFootage": "parc_sq_ft", "acquisitionMethod": "acquisition_method",
          "acquisitionDate": "acquisition_date", "propertyClass": "class", "censusTract": "census_tract",
          "latitude": "lat", "longitude": "long", "inventoryType": "inventory_type", "zonedAs": "zoned_as",
          "currentStatus": "current_status", "statusDate": "status_date_utc"}

json_bucket = f"{os.environ['GCS_PREFIX']}_eproperty"

parser = argparse.ArgumentParser()
parser.add_argument('--json_output_arg', dest='json_out_loc', required=True,
                    help='fully specified location to upload the processed json file for long-term storage')
args = vars(parser.parse_args())

# Build the API request components
URL_BASE = F"https://api.epropertyplus.com/landmgmt/api/property/"
URL_QUERY = F"/summary?"
url = URL_BASE + URL_QUERY

header = {"Host": "api.epropertyplus.com",
          "Connection": "keep-alive",
          "x-strllc-authkey": F"{os.environ['EPROPERTY_TOKEN']}",
          "User-Agent": "Mozilla/5.0(Macintosh;IntelMacOSX10_9_2...",
          "Content-Type": "application/json",
          "Accept": "*/*",
          "Accept-Encoding": "gzip, deflate, sdch",
          "Accept-Language": "en - US, en;q = 0.8"}

params = {"page": 1, "limit": API_LIMIT}

# Hit the API
all_records = []
more = True
while more is True:
    # API call to get data
    response = requests.get(url, headers=header, params=params)

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

drops = [f for f in df_records.columns.to_list() if f not in FIELDS.keys()]
df_records.drop(drops, axis=1, inplace=True)

df_records.rename(columns=FIELDS, inplace=True)
# re order columns
df_records = df_records[FIELDS.values()]

# convert id, an int, to string to be consistent with our SOP and change NaNs to Null
df_records["id"] = df_records["id"].astype(str)
df_records["address"].apply(lambda x: x.upper())

# clean the parcel numbers
df_records["parc"] = df_records["parc"].apply(lambda x: str(x.strip().upper()))
regex = re.compile('[@_!#$%^&*()<>?/\|}{~:]')
df_records.drop(
        df_records[df_records["parc"].apply(lambda x: regex.search(x)) == None].index.to_list(),
inplace = True
)
df_records.drop(
        df_records[df_records["parc"].apply(lambda x: len(x) > 16)].index.to_list(),
        inplace = True
)
df_clean = df_records[df_records["parc"].apply(lambda x: len(x) == 16)]
df_bad_parc = df_records[df_records["parc"].apply(lambda x: len(x) < 16)]
df_bad_parc["parc"] = df_bad_parc["parc"].apply(lambda x: normalize_block_lot(x))
df_bad_parc = df_bad_parc[   df_bad_parc["parc"].apply(lambda x: x != None)]

df_final = df_clean.append(df_bad_parc)


# load into BQ
schema = conv_avsc_to_bq_schema(F"{os.environ['GCS_PREFIX']}_avro_schemas", "eproperty_vacant_property.avsc")
df_final.to_gbq("eproperty.vacant_properties", F"{os.environ['GCLOUD_PROJECT']}", if_exists="replace",
                  table_schema=schema)

# load API results as a json to GCS autoclass storage and avro to temporary hot storage bucket (deleted after load
# into BQ)
list_of_dict_recs = df_records.to_dict(orient='records')
json_to_gcs(args['json_out_loc'], list_of_dict_recs, json_bucket)



