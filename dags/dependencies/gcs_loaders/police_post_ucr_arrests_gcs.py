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
                          os.environ['APRS_DB'],
                          [os.environ['APRS_UN'], os.environ['APRS_PW']],
                          os.environ['DAGS_PATH'] + "/dependencies/gcs_loaders/ojdbc6.jar")

#TODO: use args['prev_execution_date'] param to limit this query temporally

blotter_query = F"""
                SELECT
                    a.arrest_id,
                    is_sex_Crime (A.ARREST_ID, 'ARREST') AS Sex_Crime,
                    A.INCIDENT_NBR,
                    Get_blotter_Person_Age (a.arrest_id, 'ARREST') AS age,
                    Get_blotter_Person_Gender (a.arrest_id, 'ARREST') AS Gender,
                    Get_blotter_Person_Race (a.arrest_id, 'ARREST') AS Race,
                    TO_CHAR (a.arrest_datetime, 'mm/dd/yyyy HH24:MI') arrest_time,
                CASE
                    WHEN is_sex_Crime (A.ARREST_ID, 'ARREST') = 'Y' THEN get_zone_for_sexcrime (ad.address_id, 'ARREST')
                ELSE get_blotter_address (ad.address_id, 'ARREST') END AS ArrestLocation,
                    get_blotter_offenses (a.arrest_id, 'ARREST') AS Offenses,
                CASE
                    WHEN is_sex_Crime (A.ARREST_ID, 'ARREST') = 'Y' THEN get_arr_incident_Zone_sexCrime (a.incident_nbr)
                ELSE get_arr_incident_address (a.incident_nbr) END AS IncidentLocation,
                CASE
                    WHEN is_sex_Crime (A.ARREST_ID, 'ARREST') = 'Y' THEN NULL
                    ELSE get_arr_incident_Neighborhood (a.incident_nbr) END AS IncidentNeighborhood,
                get_arr_incident_Zone (a.incident_nbr) AS IncidentZone,
                CASE
                    WHEN is_sex_Crime (A.ARREST_ID, 'ARREST') = 'Y' THEN NULL
                    ELSE get_arr_incident_Census (a.incident_nbr) END AS INCIDENTTRACT
                FROM
                    arrest a,
                    address ad
                WHERE
                    a.arrest_id = ad.parent_table_id
                    AND ad.parent_table_name = 'ARREST'
                    AND ad.parent_Table_column = 'ARREST_ID'
                    AND ad.address_type = 'ARREST'
            """

arrests = sql_to_dict_list(conn, blotter_query, db='oracle')

conn.close()

json_to_gcs('30_day_blotter/{}/{}/{}_post_ucr_arrests.json'.format(args['execution_date'].split('-')[0],
                                                                   args['execution_date'].split('-')[1],
                                                                   args['execution_date']),
            arrests, bucket)
