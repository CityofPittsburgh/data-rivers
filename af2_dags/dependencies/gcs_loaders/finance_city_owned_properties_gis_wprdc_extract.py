import os
import argparse

import re
import jaydebeapi
import pandas as pd

from gcs_utils import json_to_gcs, sql_to_df, conv_avsc_to_bq_schema

# Note: we recently (5/23) learned that this pipeline is end of life with 6 months of creation (11/23). While more
# data enrichment and work would normally be completed, this is sufficient, given this situation.
# column name changes
NEW_NAMES = {"PIN": "pin", "ADDRESS": "address", "BLOCK_LOT": "block_lot"}


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
  master.prop_low_house_no || ' ' || master.prop_street_name ADDRESS, 
  master.block_lot
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

# convert date to same format as in the timebound geo tables and also get the UTC time
# the source dates are odd in this case. they are UTC, with a meaningless designation of midnight for all timestamps
# (the data were not altered at midnight in actuality). We want to add EST times as part of our SOP, but a normal
# derivation of EST would make the date one day prior to the real UTC date. We copy EST directly from UTC here,
# with the understanding that both timestamps are incorrect
data["approved_date_UNIX"] = pd.to_datetime(data["approved_date"]).map(pd.Timestamp.timestamp).astype(int)
data["approved_date_UTC"] = data["approved_date"].map(lambda x: x[:10])
data["approved_date_EST"] = data["approved_date"].map(lambda x: x[:10])
data.drop("approved_date", axis = 1, inplace = True)


# load into BQ via the avsc file in schemas direc
schema = conv_avsc_to_bq_schema(F"{os.environ['GCS_PREFIX']}_avro_schemas", "tax_abatement.avsc")
data.to_gbq("finance.incoming_tax_abatement", project_id=f"{os.environ['GCLOUD_PROJECT']}",
            if_exists="replace", table_schema=schema)

# load query results as a json to GCS autoclass storage for archival
list_of_dict_recs = data.to_dict(orient='records')
json_to_gcs(f"{args['out_loc']}", list_of_dict_recs, F"{os.environ['GCS_PREFIX']}_finance")