import argparse
import os

import re
import jaydebeapi
import numpy as np
import pandas as pd

from gcs_utils import json_to_gcs, sql_to_df, conv_avsc_to_bq_schema

# Note: we recently (5/23) learned that this pipeline is end of life with 6 months of creation (11/23). While more
# data enrichment and work would normally be completed, this is sufficient, given this situation.
# column name changes
NEW_NAMES = {"PIN": "pin", "ADDRESS": "address", "BLOCK_LOT": "block_lot", "LAST_SALE_DATE": "last_sale_date",
             "SALE_PRICE": "sale_price"}


parser = argparse.ArgumentParser()
parser.add_argument('--output_arg', dest='out_loc', required=True,
                    help='fully specified location to upload the output of the SQL query')
args = vars(parser.parse_args())


# build connection to the DB which will be used in the utility func below
conn = jaydebeapi.connect("oracle.jdbc.OracleDriver", os.environ['REALESTATE_DB'],
                          [os.environ['REALESTATE_UN'], os.environ['REALESTATE_PW']],
                          f"{os.environ['GCS_LOADER_PATH']}/ojdbc6.jar")


# build query
# master.neighborhood,
query = """
SELECT DISTINCT
  city_county_accounts.cnty_acct PIN,
  master.prop_low_house_no || ' ' || master.prop_street_name || ', ' || MASTER.PROP_CITY || ', ' || MASTER.PROP_STATE || ' ' || MASTER.PROP_ZIP  ADDRESS, 
  master.block_lot,
  master.last_sale_date,
  master.sale_price
FROM
  master, 
  city_county_accounts
WHERE
  MASTER.CNTY_OWNER_NAME = 'CITY OF PITTSBURGH'
  and
  master.acct_no = city_county_accounts.city_acct
"""

# execute query
data = sql_to_df(conn, query, db = os.environ['REALESTATE_DRIVER'])

# data cleaning:
# rename columns
data.rename(columns = NEW_NAMES, inplace = True)

# strip leading 0's from addresses (e.g., 0 MAIN ST should just become MAIN ST)
data['address'] = data['address'].apply(lambda x: re.sub(r'^0\s', '', x) if isinstance(x, str) else x)

# convert 0 dollar sales to null values
data.loc[data["sale_price"] == 0, "sale_price"] = np.nan

# change null dates to UNIX time = 0 (1970-01-01) so that ALL valid dates can be converted to UNIX & UTC time. W/o
# this conversion a type error is thrown. There is an abnormality with this date -> the date is extracted with a full
# date time (always set 00:00:00), which is incorrect. Therefore, drop the incorrect values from the string. Since
# the date is almost certainly not technically midnight in the literal sense, we will set EST and UTC to identical
# values. While this is redundant this keeps timezone specifications consistent with our SOPs.
data.loc[data["last_sale_date"].isnull(), "last_sale_date"] = "1970-01-01 00:00:00"
data["last_sale_date_UNIX"] = pd.to_datetime(data["last_sale_date"]).map(pd.Timestamp.timestamp).astype(int)

data["last_sale_date_UTC"] = data["last_sale_date"].map(lambda x: x[:10])

data.loc[data["last_sale_date_UNIX"] == 0, ["last_sale_date_UNIX", "last_sale_date_UTC"]] = np.nan

data["last_sale_date_EST"] = data["last_sale_date_UTC"].copy()

data.drop("last_sale_date", axis = 1, inplace = True)

# load into BQ via the avsc file in schemas direc
schema = conv_avsc_to_bq_schema(F"{os.environ['GCS_PREFIX']}_avro_schemas", "city_owned_properties.avsc")
data.to_gbq("finance.incoming_city_owned_properties", project_id=f"{os.environ['GCLOUD_PROJECT']}",
            if_exists="replace", table_schema=schema)

# load query results as a json to GCS autoclass storage for archival
list_of_dict_recs = data.to_dict(orient='records')
json_to_gcs(f"{args['out_loc']}", list_of_dict_recs, F"{os.environ['GCS_PREFIX']}_finance")