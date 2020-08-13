import argparse
import csv
import os

import pymssql

from gcs_utils import mssql_to_dict_list, json_to_gcs

parser = argparse.ArgumentParser()
parser.add_argument('-e', '--execution_date', dest='execution_date',
                    required=True, help='DAG execution date (YYYY-MM-DD)')
args = vars(parser.parse_args())


bucket = '{}_community_centers'.format(os.environ['GCS_PREFIX'])
conn = pymssql.connect(host=os.environ['RECPRO_DB'], user=os.environ['RECPRO_UN'],
                       password=os.environ['RECPRO_PW'], database='recpro')

# TODO: centers query + geocode in airflow script

attendance_query = """
                    SELECT CAST(DATEADD(DAY, DATEDIFF(DAY, 0, MemUse.Date_Time), 0) AS DATE) AS Date, 
                           Center.Name as CenterName, COUNT(MemUse.CardNumber) AttendanceCount   
                    FROM [recpro].[dbo].[MembershipUse] as MemUse 
                    LEFT JOIN [recpro].[dbo].[Facilities] as Center ON MemUse.Location = Center.ID 
                    WHERE MemUse.Date_Time < DATEDIFF(DAY, 0, GETDATE()) 
                          AND MemUse.Date_Time > '2011-03-06' 
                          AND Center.Name IS NOT NULL 
                    GROUP BY DATEADD(DAY, DATEDIFF(DAY, 0, MemUse.Date_Time), 0), Center.Name 
                    ORDER BY Date DESC;
                 """

attendance_results = mssql_to_dict_list(conn, attendance_query, date_col='Date', date_format='%y-%m-%d')

conn.close()

json_to_gcs('attendance/{}/{}/{}_attendance.json'.format(args['execution_date'].split('-')[0],
                                                         args['execution_date'].split('-')[1],
                                                         args['execution_date']),
            attendance_results, bucket)
