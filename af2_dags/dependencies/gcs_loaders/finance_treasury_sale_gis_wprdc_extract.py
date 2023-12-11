import argparse
import os

import re
import jaydebeapi

from gcs_utils import json_to_gcs, sql_to_df, conv_avsc_to_bq_schema

# Note: we recently (5/23) learned that this pipeline is end of life within 6 months of creation (11/23). While more
# data enrichment and work would normally be completed, this is sufficient, given this situation.


parser = argparse.ArgumentParser()
parser.add_argument('--output_arg', dest='out_loc', required=True,
                    help='fully specified location to upload the output of the SQL query')
args = vars(parser.parse_args())


# build connection to the DB which will be used in the utility func below
conn = jaydebeapi.connect("oracle.jdbc.OracleDriver", os.environ['REALESTATE_DB'],
                          [os.environ['REALESTATE_UN'], os.environ['REALESTATE_PW']],
                          f"{os.environ['GCS_LOADER_PATH']}/ojdbc6.jar")

# build query
# The upcoming t sales are in the tsd table. The jt table contains these properties and many others. There is a great
# deal of redundancy between these tables. The decision to use the tax due figures from the jt table is somewhat
# arbitrary as these figures are identical. The cca table simply contains account numbers. These account numbers are
# different depictions of block/lot or parcel number. We use the county's format and select that here. The m table
# contains the upcoming sale date. This column appears in the tsd table, but is always Null. It does not
# appear in the jt table. Address can be derived from several tables and the m table choice is arbitrary. T_SALE_FLAG
# is a critical field. If a property is designated for sale then this field (only found in the m table) is set to
# "Y". If a payment plan is set up then the flag will be set to "N" and the property is no longer designated for T
# Sale. Properties can move from Y to N as settlements are made. Occassionally, a property can fluctuate between Y and
# N as the payment is being processed, and this is a transient disruption.
#
# Join 1 excludes props that aren't in the upcoming sale (this restricts dataset to relevant properties)
# Join 2 gets the parcel number based on the city account number (enrichment of data from 1st join)
# Join 3 gets the sale date and sale flag (further enrichment)
query = """
SELECT 
	cca.CNTY_ACCT AS parc_num,
	m.TREAS_SALE_DATE,
	jt.TOTAL_JORDAN_DUE AS total_tax_due, 
	jt.CITY_JORDAN_DUE AS city_tax_due,
	jt.SCHOOL_JORDAN_DUE AS school_tax_due,
	jt.COUNTY_JORDAN_DUE AS county_tax_due,
	jt.LIBRARY_JORDAN_DUE AS library_tax_due,
	jt.PWSA_JORDAN_DUE AS pwsa_tax_due,
	jt.DEMOLITION_COST_DUE AS demo_cost_due,
	jt.OTHER_MUNICIPAL_CHARGES_DUE AS clean_lien_due,
	jt.PARKS_JORDAN_DUE AS parks_tax_due,
	m.TREAS_SALE_FLAG, 
  	m.prop_low_house_no || ' ' || m.prop_street_name || ', ' || m.PROP_CITY || ', ' || m.PROP_STATE || ' ' || m.PROP_ZIP AS ADDRESS
FROM 
	JORDAN_TSALE jt
INNER JOIN TREAS_SALE_DELINQUENT tsd ON
jt.ACCT_NO = tsd.ACCT_NO 
INNER JOIN CITY_COUNTY_ACCOUNTS cca ON 
cca.CITY_ACCT = jt.ACCT_NO 
INNER JOIN MASTER m ON
jt.ACCT_NO  = m.ACCT_NO"""

# execute query
df = sql_to_df(conn, query, db = os.environ['REALESTATE_DRIVER'])

# data cleaning:
# rename columns
df.columns = df.columns.str.lower().to_list()
data.rename(columns = {'pin': 'parc_num'}, inplace = True)

# strip leading 0's from addresses (e.g., 0 MAIN ST should just become MAIN ST)
df['address'] = df['address'].apply(lambda x: re.sub(r'^0\s', '', x) if isinstance(x, str) else x)

# convert all dates to YYYY-MM-DD (or None) and change col name
df["treas_sale_date"] = df["treas_sale_date"].apply(lambda x: str(x[:10]) if x is not None else None)

# load into BQ via the avsc file in schemas direc
schema = conv_avsc_to_bq_schema(F"{os.environ['GCS_PREFIX']}_avro_schemas", "treasury_sale_properties.avsc")
df.to_gbq("finance.incoming_treas_sale", project_id = f"{os.environ['GCLOUD_PROJECT']}", if_exists = "replace",
          table_schema = schema)

# load query results as a json to GCS autoclass storage for archival needs
list_of_dict_recs = df.to_dict(orient = 'records')
json_to_gcs(f"{args['out_loc']}", list_of_dict_recs, F"{os.environ['GCS_PREFIX']}_finance")
