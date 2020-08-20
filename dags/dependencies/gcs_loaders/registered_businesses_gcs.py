import os
import argparse

import jaydebeapi

from gcs_utils import sql_to_dict_list, json_to_gcs

parser = argparse.ArgumentParser()
parser.add_argument('-e', '--execution_date', dest='execution_date',
                    required=True, help='DAG execution date (YYYY-MM-DD)')
args = vars(parser.parse_args())

bucket = '{}_finance'.format(os.environ['GCS_PREFIX'])

conn = jaydebeapi.connect("oracle.jdbc.OracleDriver",
                          os.environ['ISAT_DB'],
                          [os.environ['ISAT_UN'], os.environ['ISAT_PW']],
                          os.environ['DAGS_PATH'] + "/dependencies/gcs_loaders/ojdbc6.jar")

businesses_query = """
                    SELECT DISTINCT acct.NAME, 
                                    Regexp_replace(biz.trade_name, '[,\\s]+|[,\\s]+', ' ') 
                                    trade_name, 
                                    Regexp_replace(biz.desc_of_business, '[,\\s]+|[,\\s]+', ' ') 
                                    desc_of_business, 
                                    biz.business_type, 
                                    con.address_type, 
                                    con.street_number, 
                                    Regexp_replace(con.street_half, '[,\\s]+|[,\\s]+', ' ') 
                                    street_half, 
                                    Regexp_replace(con.street_name, '[,\\s]+|[,\\s]+', ' ') 
                                    street_name, 
                                    Regexp_replace(con.street_suffix, '[,\\s]+|[,\\s]+', ' ') 
                                    street_suffix, 
                                    Regexp_replace(con.apt_no, '[,\\s]+|[,\\s]+', ' ') 
                                    apt_no, 
                                    Regexp_replace(con.misc_address_line, '[,\\s]+|[,\\s]+', ' ') 
                                    misc_address_line, 
                                    Regexp_replace(con.city, '[,\\s]+|[,\\s]+', ' ') 
                                    city, 
                                    con.state_code, 
                                    con.zip, 
                                    acct.date_created, 
                                    biz.business_start_date_in_pgh 
                    FROM PITSDBA.it_tbl_m_taxpayer_acct acct 
                           INNER JOIN PITSDBA.it_tbl_m_taxpayer_business biz 
                                   ON acct.acct_no = biz.acct_no 
                           LEFT JOIN(SELECT inn. * 
                                     FROM (SELECT t2. *, 
                                                   ( Row_number() 
                                                       OVER( 
                                                         partition BY acct_no 
                                                         ORDER BY Decode(address_type, 'CL', 'CA', 
                                                       'NCL', 
                                                       'RA', 'RP' 
                                                       , 'TA', 
                                                       'MA', 
                                                       'OA', 'PRL')) ) rank 
                                            FROM   PITSDBA.it_tbl_m_taxpayer_contact t2) inn 
                                     WHERE inn.rank = 1 
                                            AND status_address = 'V') con 
                                  ON acct.acct_no = con.acct_no 
                    WHERE acct.acct_status = 'A' 
                        AND acct.account_type = 'B' 
                    ORDER BY acct.date_created ASC 
                   """

businesses = sql_to_dict_list(conn, businesses_query, db='oracle')

conn.close()

json_to_gcs('businesses/{}/{}/{}_businesses.json'.format(args['execution_date'].split('-')[0],
                                                         args['execution_date'].split('-')[1],
                                                         args['execution_date']),
            businesses, bucket)
