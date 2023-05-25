import os
import argparse
import re
import jaydebeapi
import pendulum
from datetime import datetime
from google.cloud import storage
from gcs_utils import find_last_successful_run, json_to_gcs, sql_to_df, conv_avsc_to_bq_schema


conn = jaydebeapi.connect("oracle.jdbc.OracleDriver", os.environ['REALESTATE_DB'],
                          [os.environ['REALESTATE_UN'], os.environ['REALESTATE_PW']],
                          f"{os.environ['GCS_LOADER_PATH']}/ojdbc6.jar")

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


data = sql_to_df(conn, query, db=os.environ['REALESTATE_DRIVER'])

