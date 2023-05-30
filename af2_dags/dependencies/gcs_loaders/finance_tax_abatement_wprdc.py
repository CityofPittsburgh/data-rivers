import os
import argparse
import re
import jaydebeapi
from gcs_utils import json_to_gcs, sql_to_df, conv_avsc_to_bq_schema

# Note: we recently (5/23) learned that this pipeline is end of life with 6 months of creation (11/23). While more
# data enrichment and work would normally be completed, this is sufficient, given this situation.

# "NEIGHBORHOOD":"neighborhood",
# column name changes
NEW_NAMES = {"PIN": "pin", "ADDRESS": "address", "START_YEAR": "start_year",
             "APPROVED_DATE": "approved_date", "PROGRAM_NAME": "program_name", "NO_YEARS": "num_years",
             "ABATEMENT_AMT": "abatement_amount"}

parser = argparse.ArgumentParser()
parser.add_argument('--output_arg', dest='out_loc', required=True,
                    help='fully specified location to upload the output of the SQL query')
args = vars(parser.parse_args())


# build connection to the DB which will be used in the utility func below
conn = jaydebeapi.connect("oracle.jdbc.OracleDriver", os.environ['REALESTATE_DB'],
                          [os.environ['REALESTATE_UN'], os.environ['REALESTATE_PW']],
                          f"{os.environ['GCS_LOADER_PATH']}/ojdbc6.jar")






#master.neighborhood,




# build query
query = """SELECT DISTINCT
    city_county_accounts.cnty_acct PIN,
    master.prop_low_house_no || ' ' || master.prop_street_name ADDRESS,
    start_year,
    approved_date,
    abatement_programs.program_name,
    abatement_programs.no_years,
    abatement_programs.city_amt ABATEMENT_AMT
FROM
    account_abatements,
    abatement_programs,
    master,
    city_county_accounts
WHERE
    account_abatements.abatement_key = abatement_programs.rec_no
AND
    master.master_seq = account_abatements.master_seq
AND
    master.acct_no = city_county_accounts.city_acct"""

# execute query
data = sql_to_df(conn, query, db = os.environ['REALESTATE_DRIVER'])

# data cleaning:
# rename columns
data.rename(columns = NEW_NAMES, inplace = True)

# convert neighborhood names to match WPRDC standards
# ngh_convs = {
#     'BANKSVILLE CITY': 'BANKSVILLE',
#     'ALLENTOWN SLOPES': 'ALLENTOWN',
#     'ARLINGTON FLATS': 'ARLINGTON',
#     'ARLINGTON SLOPES': 'ARLINGTON',
#     'BLOOMFIELD BUSINESS DISTR': 'BLOOMFIELD',
#     'SHADYSIDE BUSINESS DISTR': 'SHADYSIDE',
#     'DOWNTOWN': 'CENTRAL BUSINESS DISTRICT'
# }
# data['neighborhood'] = data['neighborhood'].replace(ngh_convs)
# data['neighborhood'] = data['neighborhood'].str.title()

# strip leading 0's from addresses (e.g., 0 MAIN ST should just become MAIN ST)
data['address'] = data['address'].apply(lambda x: re.sub(r'^0\s', '', x) if isinstance(x, str) else x)

# load into BQ via the avsc file in schemas direc
schema = conv_avsc_to_bq_schema(F"{os.environ['GCS_PREFIX']}_avro_schemas", "tax_abatement.avsc")
data.to_gbq("finance.tax_abatement", project_id=f"{os.environ['GCLOUD_PROJECT']}",
            if_exists="replace", table_schema=schema)

# load query results as a json to GCS autoclass storage for archival
list_of_dict_recs = data.to_dict(orient='records')
json_to_gcs(f"{args['out_loc']}", list_of_dict_recs, F"{os.environ['GCS_PREFIX']}_finance")