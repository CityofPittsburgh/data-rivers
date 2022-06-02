import os
import argparse

import jaydebeapi

from gcs_utils import sql_to_dict_list, json_to_gcs


"""
I haven't actually tested this in prod so there may be a bug or two, and the DAG file remains to be written, but 
here's a start!
 — james 11/10/20
"""

parser = argparse.ArgumentParser()
parser.add_argument('-e', '--execution_date', dest='execution_date',
                    required=True, help='DAG execution date (YYYY-MM-DD)')
parser.add_argument('-s', '--prev_execution_date', dest='prev_execution_date',
                    required=True, help='Previous DAG execution date (YYYY-MM-DD)')
args = vars(parser.parse_args())

bucket = '{}_police'.format(os.environ['GCS_PREFIX'])

conn = jaydebeapi.connect("oracle.jdbc.OracleDriver",
                          os.environ['RMSPROD_DB'],
                          [os.environ['RMSPROD_UN'], os.environ['RMSPROD_PW']],
                          os.environ['DAGS_PATH'] + "/dependencies/gcs_loaders/ojdbc6.jar")

blotter_query = F"""
                SELECT
                    NTC.citation_non_traffic_id,
                    ntc.incident_nbr,
                    Get_blotter_Person_Gender (NTC.CITED_PERSON_ID,
                    'NTC') AS Gender,
                    Get_blotter_Person_Race (ntc.cited_person_id,
                    'NTC') AS Race,
                    Calculate_NTC_Person_Age (ntc.citation_non_traffic_id) AS Age,
                    TO_CHAR (ntc.reported_datetime, 'mm/dd/yyyy HH24:MI') cited_time,
                    get_blotter_address (ntc.occurrence_address_id, 'NTC') AS IncidentLocation,
                    get_blotter_offenses (ntc.citation_non_traffic_id, 'NTC') AS Offenses,
                    get_blotter_NTC_geographics(ntc.incident_nbr,
                    NTC.CITATION_NON_TRAFFIC_NBR) AS Neighborhood,
                    get_blotter_zone (ntc.occurrence_address_id) AS ZONE,
                    get_blotter_NTC_census (ntc.incident_nbr,
                    NTC.CITATION_NON_TRAFFIC_NBR) AS Census
                FROM
                    citation_non_Traffic ntc
                WHERE
                    ntc.CITED_TIME >= {args['prev_execution_date']}
                    
        """

## Line 48 may not be the right syntax for date in oracle sql

citations = sql_to_dict_list(conn, blotter_query, db='oracle')

conn.close()

json_to_gcs('30_day_blotter/{}/{}/{}_non_traffic_citations.json'.format(args['execution_date'].split('-')[0],
                                                                        args['execution_date'].split('-')[1],
                                                                        args['execution_date']),
            citations, bucket)
