import os
import argparse


from gcs_utils import sql_to_dict_list, json_to_gcs, rmsprod_setup

args, year, month, date, bucket, conn = rmsprod_setup()

demographics_query = """
SELECT
	od.GENDER AS sex,
	od.ETHNICITY AS race,
	o.OFFICER_SQUAD AS squad,
	o.OFFICER_RANK AS rank,
	o.ACTIVE_FLAG AS currently_employed,
	EXTRACT(YEAR FROM o.HIRE_DATE) AS hire_yr, 
	EXTRACT(YEAR FROM o.TERMINATION_DATE) AS termination_yr,
	EXTRACT(YEAR FROM o.DATE_OF_BIRTH) AS birth_yr
FROM
	PRMS.OFFICER_DEMOGRAPHICS od
JOIN PRMS.OFFICER o ON
	o.OFFICER_ID = od.OFFICER_ID
"""

demographics = sql_to_dict_list(conn, demographics_query, db='oracle')

conn.close()

json_to_gcs('officer_demographics/{}/{}/{}_demographics.json'.format(year, month, date), demographics, bucket)
