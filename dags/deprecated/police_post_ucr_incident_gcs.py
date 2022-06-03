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

#TODO: pass through args['prev_execution_date'] param to bound query temporally

blotter_query = F"""
                SELECT
                    i.incident_id,
                    is_sex_Crime(i.incident_id, 'INCIDENT') AS Sex_Crime,
                    i.incident_nbr AS ccr,
                    NVL (get_ucr_Hierarchy(i.incident_id), 0) AS HIERARCHY,
                    TO_CHAR (i.occurred_to_datetime, 'mm/dd/yyyy HH24:MI'),
                    CASE
                        WHEN is_sex_Crime(i.incident_id, 'INCIDENT') = 'Y' THEN get_sex_crime_zone (i.occurrence_address_id)
                        ELSE get_blotter_address(i.occurrence_address_id, 'INCIDENT') END AS Location,
                    is_report_cleared (i.incident_nbr, i.incident_id) AS CLEARED_FLAG,
                    CASE
                        WHEN is_sex_Crime (i.incident_id, 'INCIDENT') = 'Y' THEN NULL
                        ELSE get_blotter_neighborhood (i.occurrence_address_id) END AS Neighborhood,
                    Get_Blotter_Zone(i.occurrence_address_id) AS ZONE,
                    NVL (get_ucr_Description(i.incident_id), 'NA') AS HIERARCHY_DESC,
                    get_blotter_offenses (i.incident_id, 'INCIDENT') AS Offenses,
                    CASE
                        WHEN is_sex_Crime (i.incident_id, 'INCIDENT') = 'Y' THEN NULL
                        ELSE get_blotter_census (i.occurrence_address_id) END AS TRACT
                FROM
                    incident i
                WHERE
                    i.reported_datetime >= {args['prev_execution_date']}
                """

incidents = sql_to_dict_list(conn, blotter_query, db='oracle')

conn.close()

json_to_gcs('30_day_blotter/{}/{}/{}_post_ucr_incidents.json'.format(args['execution_date'].split('-')[0],
                                                                     args['execution_date'].split('-')[1],
                                                                     args['execution_date']),
            incidents, bucket)
