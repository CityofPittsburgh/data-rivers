import argparse
import os

import re
import jaydebeapi
import pandas as pd
import numpy as np

from gcs_utils import json_to_gcs, sql_to_df, conv_avsc_to_bq_schema


# Note: we recently (5/23) learned that this pipeline is end of life with 6 months of creation (11/23). While more
# data enrichment and work would normally be completed, this is sufficient, given this situation.
# column name changes


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

        # for each part
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


NEW_NAMES = {"TSD_ACCT"          : "t_sale_acct",
             "JT_ACCT"           : "jordan_t_sale_acct",
             "TREAS_SALE_DATE"   : "t_sale_date",
             "CITY_JORDAN_DUE"   : "city_tax_due",
             "SCHOOL_JORDAN_DUE" : "school_tax_due",
             "COUNTY_JORDAN_DUE" : "county_tax_due",
             "LIBRARY_JORDAN_DUE": "library_tax_due",
             "PWSA_JORDAN_DUE"   : "pwsa_tax_due",
             "TREAS_SALE_FLAG"   : "t_sale_flag",
             "BLOCK_LOT"         : "block_lot",
             "ADDRESS"           : "address"}

# parser = argparse.ArgumentParser()
# parser.add_argument('--output_arg', dest='out_loc', required=True,
#                     help='fully specified location to upload the output of the SQL query')
# args = vars(parser.parse_args())


# build connection to the DB which will be used in the utility func below
conn = jaydebeapi.connect("oracle.jdbc.OracleDriver", os.environ['REALESTATE_DB'],
                          [os.environ['REALESTATE_UN'], os.environ['REALESTATE_PW']],
                          f"{os.environ['GCS_LOADER_PATH']}/ojdbc6.jar")

# build query
query = """SELECT 
	city_county_accounts.cnty_acct PIN,
	tsd.ACCT_NO AS tsd_acct,
	jt.ACCT_NO AS jt_acct,
	m.TREAS_SALE_DATE,
	jt.CITY_JORDAN_DUE,
	jt.SCHOOL_JORDAN_DUE,
	jt.COUNTY_JORDAN_DUE,
	jt.LIBRARY_JORDAN_DUE,
	jt.PWSA_JORDAN_DUE,
	m.TREAS_SALE_FLAG, 
	m.BLOCK_LOT,
  	m.prop_low_house_no || ' ' || m.prop_street_name || ', ' || m.PROP_CITY || ', ' || m.PROP_STATE || ' ' || 
  	m.PROP_ZIP AS ADDRESS
FROM 
	JORDAN_TSALE jt
LEFT OUTER JOIN TREAS_SALE_DELINQUENT tsd ON
jt.ACCT_NO = tsd.ACCT_NO 
INNER JOIN MASTER m ON
jt.ACCT_NO  = m.ACCT_NO
INNER JOIN city_county_accounts ON 

WHERE tsd.ACCT_NO IS NOT NULL
"""

query = """"WITH tsale_props AS
(SELECT 
	tsd.ACCT_NO AS tsd_acct,
	jt.ACCT_NO AS jt_acct,
	m.ACCT_NO AS master_acct,
	m.TREAS_SALE_DATE,
	jt.CITY_JORDAN_DUE,
	jt.SCHOOL_JORDAN_DUE,
	jt.COUNTY_JORDAN_DUE,
	jt.LIBRARY_JORDAN_DUE,
	jt.PWSA_JORDAN_DUE,
	m.TREAS_SALE_FLAG, 
	m.BLOCK_LOT,
  	m.prop_low_house_no || ' ' || m.prop_street_name || ', ' || m.PROP_CITY || ', ' || m.PROP_STATE || ' ' || m.PROP_ZIP AS ADDRESS
FROM 
	JORDAN_TSALE jt
LEFT OUTER JOIN TREAS_SALE_DELINQUENT tsd ON
jt.ACCT_NO = tsd.ACCT_NO 
INNER JOIN MASTER m ON
jt.ACCT_NO  = m.ACCT_NO
WHERE tsd.ACCT_NO IS NOT NULL)

SELECT 
	cca.CITY_ACCT AS PIN
	tsale_props.*
FROM 
	CITY_COUNTY_ACCOUNTS cca 
	tsale_props
WHERE cca.CITY_ACCT = tsale_props.master_acct
"""

# execute query
df = sql_to_df(conn, query, db = os.environ['REALESTATE_DRIVER'])

# data cleaning:
# rename columns
df.rename(columns = NEW_NAMES, inplace = True)

# strip leading 0's from addresses (e.g., 0 MAIN ST should just become MAIN ST)
df['address'] = df['address'].apply(lambda x: re.sub(r'^0\s', '', x) if isinstance(x, str) else x)

# convert all dates to YYYY-MM-DD (or None) and change col name
df["t_sale_date"] = df["t_sale_date"].apply(lambda x: str(x[:10]) if x is not None else None)

# load into BQ via the avsc file in schemas direc
schema = conv_avsc_to_bq_schema(F"{os.environ['GCS_PREFIX']}_avro_schemas", "treasury_sale_properties.avsc")
df.to_gbq("finance.incoming_treas_sale", project_id = f"{os.environ['GCLOUD_PROJECT']}",
          if_exists = "replace", table_schema = schema)

# load query results as a json to GCS autoclass storage for archival needs
list_of_dict_recs = df.to_dict(orient = 'records')
json_to_gcs(f"{args['out_loc']}", list_of_dict_recs, F"{os.environ['GCS_PREFIX']}_finance")

# scratch below here
# d_ty = df.columns.to_list()
# ty = [type(df[t][0]) for t in d_ty ]
