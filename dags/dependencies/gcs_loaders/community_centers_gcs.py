import pymssql
import csv
import os

from gcs_utils import upload_file_gcs, now

bucket = '{}_community_centers'.format(os.environ['GCS_PREFIX'])
conn = pymssql.connect(host=os.environ['RECPRO_DB'], user=os.environ['RECPRO_UN'],
                       password=os.environ['RECPRO_PW'], database='recpro')
cursor = conn.cursor()

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

cursor.execute(attendance_query)

with open('daily_attendance.csv', 'w') as file:
    writer = csv.writer(file, delimiter=',', lineterminator='\n', quotechar='"')
    writer.writerow([i[0] for i in cursor.description])
    writer.writerows(cursor)
    for row in cursor:
        writer.writerow(row)

upload_file_gcs(bucket, 'daily_attendance.csv', 'attendance/{}/{}/{}_attendance.csv'.format(now.strftime('%Y'),
                                                                                            now.strftime('%m').lower(),
                                                                                            now.strftime("%Y-%m-%d")))

conn.close()
