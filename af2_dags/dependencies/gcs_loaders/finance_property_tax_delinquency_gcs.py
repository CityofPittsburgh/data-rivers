import os
import argparse
import re
import jaydebeapi
import jpype
import pendulum
from datetime import datetime
from google.cloud import storage
from gcs_utils import find_last_successful_run, json_to_gcs, sql_to_df, conv_avsc_to_bq_schema

storage_client = storage.Client()
json_bucket = f"{os.environ['GCS_PREFIX']}_finance"
hot_bucket = f"{os.environ['GCS_PREFIX']}_hot_metal"

parser = argparse.ArgumentParser()
parser.add_argument('--output_arg', dest='out_loc', required=True,
                    help='fully specified location to upload the output of the SQL query')
args = vars(parser.parse_args())

run_start_win, first_run = find_last_successful_run(json_bucket, "tax_delinquency/successful_run_log/log.json",
                                                    "2023-01-05")

conn = jaydebeapi.connect("oracle.jdbc.OracleDriver", os.environ['REALESTATE_DB'],
                          [os.environ['REALESTATE_UN'], os.environ['REALESTATE_PW']],
                          f"{os.environ['GCS_LOADER_PATH']}/ojdbc6.jar")

query = F"""SELECT cca.CNTY_ACCT AS PIN, m.MODIFY_DATE, PROP_LOCATION AS ADDRESS,
                   BILL_CITY || ', ' || BILL_STATE AS BILLING_CITY, CURRENT_DELQ, 
                   PRIOR_YEARS, STATE_DESCRIPTION, NEIGHBORHOOD
              FROM WEB_DELINQUENTS wd, MASTER m, CITY_COUNTY_ACCOUNTS cca
             WHERE wd.ACCT_NO = m.ACCT_NO
               AND m.ACCT_NO = cca.CITY_ACCT
               AND m.MODIFY_DATE > TO_DATE('{run_start_win}', 'yyyy-mm-dd')"""

data = sql_to_df(conn, query, db=os.environ['REALESTATE_DRIVER'])
data = data.rename(columns=str.lower)

ngh_convs = {
    'BANKSVILLE CITY': 'BANKSVILLE',
    'ALLENTOWN SLOPES': 'ALLENTOWN',
    'ARLINGTON FLATS': 'ARLINGTON',
    'ARLINGTON SLOPES': 'ARLINGTON',
    'BLOOMFIELD BUSINESS DISTR': 'BLOOMFIELD',
    'SHADYSIDE BUSINESS DISTR': 'SHADYSIDE',
    'DOWNTOWN': 'CENTRAL BUSINESS DISTRICT'
}
data['neighborhood'] = data['neighborhood'].replace(ngh_convs)
data['neighborhood'] = data['neighborhood'].str.title()
data['address'] = data['address'].apply(lambda x: re.sub(r'^0\s', '', x) if isinstance(x, str) else x)
data['billing_city'] = data['billing_city'].mask(data['billing_city'] == ', ', None)
data['prior_years'] = data['prior_years'].astype(str)
# load into BQ
schema = conv_avsc_to_bq_schema(F"{os.environ['GCS_PREFIX']}_avro_schemas", "property_tax_delinquency.avsc")
data.to_gbq("finance.incoming_property_tax_delinquency", project_id=f"{os.environ['GCLOUD_PROJECT']}",
            if_exists="replace", table_schema=schema)

# write the successful run information (used by each successive run to find the backfill start date)
curr_run = datetime.now(tz=pendulum.timezone('EST')).strftime("%Y-%m-%d")
successful_run = {
    "requests_retrieved": len(data),
    "since": run_start_win,
    "current_run": curr_run,
    "note": "Data retrieved between the time points listed above"
}
json_to_gcs("tax_delinquency/successful_run_log/log.json", [successful_run], json_bucket)

# load query results as a json to GCS autoclass storage for archival
list_of_dict_recs = data.to_dict(orient='records')
json_to_gcs(f"{args['out_loc']}", list_of_dict_recs, json_bucket)
