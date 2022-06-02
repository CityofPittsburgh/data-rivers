import os
import argparse

import jaydebeapi

from gcs_utils import sql_to_dict_list, json_to_gcs

"""
This job creates the 30-day police blotter table, which is continually overwritten, so there's no need to pass
an execution date arg. 

The 30-Day Police Blotter contains the most recent initial crime incident data, updated on a nightly basis. All data 
is reported at the block/intersection level, with the exception of sex crimes, which are reported at the police zone 
level. The information is "semi-refined" meaning a police report was taken, but it has not made its way through the 
court system. This data is subject to change once it is processed and republished using Uniform Crime Reporting (UCR) 
standards. The UCR coding process creates a necessary delay before processed data is available for publication. 
Therefore, the 30-Day blotter will provide information for users seeking the most current information available. 

This dataset will be continually overwritten and any records older than thirty days will be removed. Validated 
incidents will be moved to the Police Blotter Archive dataset (DAG yet to be written). Data in the archived table is 
of a higher quality and is the file most appropriate for reporting crime statistics. 
"""

parser = argparse.ArgumentParser()
parser.add_argument('-e', '--execution_date', dest='execution_date',
                    required=True, help='DAG execution date (YYYY-MM-DD)')
args = vars(parser.parse_args())

bucket = '{}_police'.format(os.environ['GCS_PREFIX'])

conn = jaydebeapi.connect("oracle.jdbc.OracleDriver",
                          os.environ['POLREPS_DB'],
                          [os.environ['POLREPS_UN'], os.environ['POLREPS_PW']],
                          os.environ['DAGS_PATH'] + "/dependencies/gcs_loaders/ojdbc6.jar")

blotter_query = """
    SELECT
        c.report_id AS PK,
        is_sex_Crime(c.report_id) AS Sex_Crime,
        c.ccr,
        NVL (GET_UCR_HIERARCHY (C.REPORT_ID),
        0) AS HIERARCHY,
        TO_CHAR (o.occurred_to_date,
        'mm/dd/yyyy HH24:MI') incident_time,
    CASE
            WHEN is_sex_Crime(c.report_id) = 'Y' THEN get_sex_crime_zone(c.ccr)
            ELSE get_block_address (c.report_id)
        END AS Location,
        is_report_cleared (C.CCR) AS CLEARED_FLAG,
    CASE
            WHEN is_sex_Crime(c.report_id) = 'Y' THEN NULL
            ELSE get_blotter_neighborhood (c.ccr)
        END AS Neighborhood,
        NVL (get_blotter_zone (c.ccr),
        'N/A') ZONE,
        NVL (GET_UCR_HIERARCHY_DESC (C.REPORT_ID),
        'NA') AS HIERARCHY_DESC,
        GET_OFFENSES (C.REPORT_ID) AS OFFENSES,
    CASE
            WHEN is_sex_Crime (c.report_id) = 'Y' THEN NULL
            ELSE get_blotter_census (c.ccr)
        END AS tract
    FROM
        rpt_control c,
        rpt_offense_incident o
    WHERE
        c.report_name = ('OFFENSE 2.0')
        AND c.accepted_flag = 'Y'
        AND TRUNC (o.occurred_to_date) >= TRUNC (SYSDATE) - 30
        AND c.report_id = o.report_id
        AND c.status IN ('REVIEWED AND ACCEPTED',
        'REVIEWED AND ACCEPTED WITH CHANGES')
        """

blotter = sql_to_dict_list(conn, blotter_query, db='oracle')

conn.close()

json_to_gcs('30_day_blotter/{}/{}/{}_blotter.json'.format(args['execution_date'].split('-')[0],
                                                          args['execution_date'].split('-')[1],
                                                          args['execution_date']),
            blotter, bucket)
