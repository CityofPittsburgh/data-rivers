import pymssql
import csv
import os

from gcs_utils import upload_file_gcs, storage_client, now

bucket = '{}_rec_centers'.format(os.environ['GCS_PREFIX'])
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

facilities_query = """
                    SELECT f.id, 
                           f.NAME, 
                           f.address1, 
                           f.zip_code, 
                           fc.description 
                    FROM   [recpro].[dbo].[facilities] AS f 
                           LEFT JOIN recpro.dbo.facilitycategories AS fc 
                                  ON f.facility_category = fc.facilitycat_id 
                   """

cursor.execute(facilities_query)

with open('facilities.csv', 'w') as file:
    writer = csv.writer(file, delimiter=',', lineterminator='\n', quotechar='"')
    writer.writerow([i[0] for i in cursor.description])
    writer.writerows(cursor)
    for row in cursor:
        writer.writerow(row)

upload_file_gcs(bucket, 'facilities.csv', 'facilities/{}/{}/{}_facilities.csv'.format(now.strftime('%Y'),
                                                                                      now.strftime('%m').lower(),
                                                                                      now.strftime("%Y-%m-%d")))

conn.close()
