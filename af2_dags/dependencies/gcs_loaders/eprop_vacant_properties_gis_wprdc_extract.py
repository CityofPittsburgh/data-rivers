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
    """
    function to standardize the format of parcel numbers/block lots to the county format. this is intended to be run on
    single input values and is designed to work in parallel processing applications (e.g. dataflow or pandas.apply etc)

    the terminology block/lot and parcel number are interchangeable

    the only acceptable format is the same the county uses- a 16 char string (no special chars) with the 5th
    character being a letter and all others numeric.

    each of the 5 components of the parcel number string are fixed in length (4, 1, 5, 4, and 2 characters, respectively)
    and no characters can be omitted (e.g. 1234X12345123412 is correct) Zero padding the components is very common (
    i.e. each component frequently contains several "0") (0001X00001000101 is correctly formatted).

    The city frequently inserts hyphens between components and does not zero pad them. As an example, this parcel
    number: 0001X00001000101 (when correctly formatted) could appear like this in the city's datasets: 01-X-01-1-1.
    Further, when all components of the parcel number are "0"s the city frequently omits them. As an example: 01-X-01
    would represent 0001X00001000000.

    This function will dehypenate, zero pad, and verify correctness

    :param x: string input value representing the parcel number
    :return out: a string of the formatted parcel number
    """

    # default parcel number component values (number of chars per component, boolean if alphabetical vals allowed,
    # boolean if the component is required (sometimes the final two components (6 chars) are ommitted)

    components = {"len"       : [4, 1, 5, 4, 2],
                  "alpha_char": [False, True, False, True, True],
                  "required"  : [True, True, True, False, False]}

    # skip Nulls
    if x is not None and x is not np.nan:

        # strip white spaces
        x = x.strip().upper()

        # all values must be a hyphen or alphanumeric (no special chars)
        for char_val in x:
            if not char_val.isalnum() and char_val != "-":
                return "invalid input"

        # either the string contains hyphens (and can be variable length) or it is exactly 16 chars
        # some city formats contain hyphens and abbreviated componentsf
        if x.__contains__("-"):
            parts = x.split("-")
            if len(parts) > len(components["required"]):
                return "invalid input"

        # if the input doesn't have hyphens (determined above) and is the proper length:
        # parts will be a list with all 5 components of the parc number
        # grab the leading characters and place them into a growing list (as seperate elements)
        # then drop those characters from the input so that only leading characters are extracted
        # the end result is each component of the input parc number is extracted and broken down for further analysis
        elif len(x) == 16:
            parts = []
            for l in components["len"]:
                sel_string = x[:l]
                x = x[l:]
                parts.append(sel_string)

        # fails above conditional logic (thus is not 16 chars long or does not contain hyphnens)
        else:
            return "invalid input"

        # all processing below assumes that the input is exactly 16 chars or hyphenated, and without special chars.
        # all fully inspected vals will overwrite the elements of conv_vals. The final 2 components of the string are
        # often ommitted by the city if they are all zeros. In this case, the zeros pre populated in conv_vals will
        # take their place
        conv_vals = ["", "", "", "0000", "00"]

        #for each part
        for i in range(len(parts)):

            # verify all chars in component are correctly alpha or non alpha components (as of August 2023,
            # only component #1 and #3 cannot be a letter) (special char containing strings already bypassed-
            # this checks that each char within each component is correctly alpha or numeric (e.g not like "12A4" in
            # the first component)

            # for each char in the selected part
            for c_num in parts[i]:
                # check if the selected character is in a component allowing letters
                if not components["alpha_char"][i] and c_num.isalpha():
                    return "invalid input"

            # verify component is not longer than allowed (if the string was 16 chars then this has to be correct,
            # so this only really is useful if the input was hyphenated. input string hyphen locations are variable)
            if len(parts[i]) > components["len"][i]:
                return "invalid input"

            # pad all string parts with zeros if they are shorter than required. this code results in no changes if
            # 1) the input string was 16 chars in length or 2) the selected part is already the correct length
            conv_vals[i] = parts[i].rjust(components["len"][i], "0")

        out = "".join(conv_vals)

        # the final output must be 16 chars (this is essentially guaranteed by this point and this is a final
        # safeguard and there must be a minimum of 2 unique characters (one letter and one number). The reality is
        # that there should be approx 4-5 unique characters as the absolute minimum. This is not well constrained in
        # the parcel number system. The minimum of 2 unique characters prevents test strings like "0000A00000000000"
        # from passing through. This is, however, only a weak safeguard.
        if len(out) == 16 and len(set(out)) > 2:
            return out
        else:
            return "invalid input"
    else:
        return "invalid input"


API_LIMIT = 10000
FIELDS = {"id"             : "id", "parcelNumber": "parc_num", "propertyAddress1": "address",
          "currentOwners"  : "owner", "parcelSquareFootage": "parc_sq_ft", "acquisitionMethod": "acquisition_method",
          "acquisitionDate": "acquisition_date", "propertyClass": "class", "censusTract": "census_tract",
          "latitude"       : "lat", "longitude": "long", "inventoryType": "inventory_type", "zonedAs": "zoned_as",
          "currentStatus"  : "current_status", "statusDate": "status_date_utc"}

json_bucket = f"{os.environ['GCS_PREFIX']}_eproperty"

parser = argparse.ArgumentParser()
parser.add_argument('--json_output_arg', dest = 'json_out_loc', required = True,
                    help = 'fully specified location to upload the processed json file for long-term storage')
args = vars(parser.parse_args())

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

drops = [f for f in df_records.columns.to_list() if f not in FIELDS.keys()]
df_records.drop(drops, axis = 1, inplace = True)

df_records.rename(columns = FIELDS, inplace = True)
# re order columns
df_records = df_records[FIELDS.values()]

# convert id, an int, to string to be consistent with our SOP and change NaNs to Null
df_records["id"] = df_records["id"].astype(str)
df_records["address"] = df_records["address"].apply(lambda x: x.upper())

# clean the parcel numbers
df_records["parc_num"] = df_records["parc_num"].apply(lambda x: normalize_block_lot(x))

# load into BQ
schema = conv_avsc_to_bq_schema(F"{os.environ['GCS_PREFIX']}_avro_schemas", "eproperty_vacant_property.avsc")
df_records.to_gbq("eproperty.vacant_properties", F"{os.environ['GCLOUD_PROJECT']}", if_exists = "replace",
                  table_schema = schema)

# load API results as a json to GCS autoclass storage and avro to temporary hot storage bucket (deleted after load
# into BQ)
list_of_dict_recs = df_records.to_dict(orient = 'records')
json_to_gcs(args['json_out_loc'], list_of_dict_recs, json_bucket)
