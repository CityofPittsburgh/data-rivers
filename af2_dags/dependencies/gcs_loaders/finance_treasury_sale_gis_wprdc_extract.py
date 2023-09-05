import argparse
import os

import re
import jaydebeapi

from gcs_utils import json_to_gcs, sql_to_df, conv_avsc_to_bq_schema

# Note: we recently (5/23) learned that this pipeline is end of life with 6 months of creation (11/23). While more
# data enrichment and work would normally be completed, this is sufficient, given this situation.
# column name changes
# NEW_NAMES = {"PIN": "pin", "ADDRESS": "address", "BLOCK_LOT": "block_lot", "SALE_PRICE": "sale_price"}
#
#
# parser = argparse.ArgumentParser()
# parser.add_argument('--output_arg', dest='out_loc', required=True,
#                     help='fully specified location to upload the output of the SQL query')
# args = vars(parser.parse_args())


# build connection to the DB which will be used in the utility func below
conn = jaydebeapi.connect("oracle.jdbc.OracleDriver", os.environ['REALESTATE_DB'],
                          [os.environ['REALESTATE_UN'], os.environ['REALESTATE_PW']],
                          f"{os.environ['GCS_LOADER_PATH']}/ojdbc6.jar")


# build query
query = """
SELECT 
	m.TREAS_SALE_DATE,
	jt.CITY_JORDAN_DUE,
	jt.SCHOOL_JORDAN_DUE,
	jt.COUNTY_JORDAN_DUE,
	jt.LIBRARY_JORDAN_DUE,
	jt.PWSA_JORDAN_DUE,
	m.BLOCK_LOT,
  	m.prop_low_house_no || ' ' || m.prop_street_name || ', ' || m.PROP_CITY || ', ' || m.PROP_STATE || ' ' || m.PROP_ZIP AS ADDRESS
FROM 
	 JORDAN_TSALE jt
LEFT OUTER JOIN TREAS_SALE_DELINQUENT tsd ON
jt.ACCT_NO = tsd.ACCT_NO 
INNER JOIN MASTER m ON
jt.ACCT_NO  = m.ACCT_NO
WHERE tsd.ACCT_NO IS NOT NULL
"""

# execute query
data = sql_to_df(conn, query, db = os.environ['REALESTATE_DRIVER'])

# data cleaning:
# rename columns
data.rename(columns = NEW_NAMES, inplace = True)

# strip leading 0's from addresses (e.g., 0 MAIN ST should just become MAIN ST)
data['address'] = data['address'].apply(lambda x: re.sub(r'^0\s', '', x) if isinstance(x, str) else x)

# convert all dates to YYYY-MM-DD (or None) and change col name
cols_conv = [{"LAST_SALE_DT": "latest_sale_date"}, {"TREAS_SALE_DT": "treasury_sale_date"},
             {"LAST_PAID_DT": "last_payment_date"}, {"GENTRIFICATION_DT": "act77_gent_date"}]
cols_list = []
for c in cols_conv:
    k = list(c.keys())[0]
    v = list(c.values())[0]
    data[v] = data[k].apply(lambda x: str(x[:10]) if x is not None else None)
    cols_list.append(k)
data.drop(cols_list, inplace = True, axis = 1)

# load into BQ via the avsc file in schemas direc
schema = conv_avsc_to_bq_schema(F"{os.environ['GCS_PREFIX']}_avro_schemas", "city_owned_properties.avsc")
data.to_gbq("finance.incoming_city_owned_properties", project_id = f"{os.environ['GCLOUD_PROJECT']}",
            if_exists = "replace", table_schema = schema)

# load query results as a json to GCS autoclass storag for archival
list_of_dict_recs = data.to_dict(orient = 'records')
json_to_gcs(f"{args['out_loc']}", list_of_dict_recs, F"{os.environ['GCS_PREFIX']}_finance")
