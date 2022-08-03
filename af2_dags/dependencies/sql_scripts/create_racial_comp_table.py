from google.cloud import bigquery, storage
import os

bq_client = bigquery.Client()
storage_client = storage.Client()

table_id = f"{os.environ['GCLOUD_PROJECT']}.ceridian.employee_vs_gen_pop_racial_comp"
job_config = bigquery.QueryJobConfig(
    destination=table_id,
    use_legacy_sql=False,
    write_disposition = 'WRITE_TRUNCATE',
    create_disposition = 'CREATE_IF_NEEDED'
)

sql = F"""-- create a table with up-to-date racial demographics for the City of Pittsburgh
          -- workforce compared to the racial demographics of the overall city population
          -- based on the 2020 US Census.
SELECT ethnicity, 100*(ethnicity_count / total) AS percentage, 'City Employee' AS type
FROM (
  SELECT IFNULL(REPLACE(ethnicity, ' (not Hispanic or Latino)', ''), 'Decline to Answer') AS ethnicity, 
  count(distinct(employee_num)) AS ethnicity_count, SUM(COUNT(*)) OVER() AS total
  FROM `{os.environ['GCLOUD_PROJECT']}.ceridian.active_employees`
  GROUP by ethnicity
)
UNION ALL
SELECT 'White' AS ethnicity, 64.5 AS percentage, 'Overall City' AS type
UNION ALL
SELECT 'Black or African American' AS ethnicity, 23.0 AS percentage, 'Overall City' AS type
UNION ALL
SELECT 'American Indian or Alaska Native' AS ethnicity, 0.2 AS percentage, 'Overall City' AS type
UNION ALL
SELECT 'Hispanic or Latino' AS ethnicity, 3.4 AS percentage, 'Overall City' AS type
UNION ALL
SELECT 'Asian' AS ethnicity, 5.8 AS percentage, 'Overall City' AS type
UNION ALL
SELECT 'Native Hawaiian or Other Pacific Islander' AS ethnicity, 0.1 AS percentage, 'Overall City' AS type
UNION ALL
SELECT 'Two or More Races' AS ethnicity, 3.6 AS percentage, 'Overall City' AS type
ORDER BY type, percentage DESC;
"""

query_job = bq_client.query(sql, job_config=job_config)
query_job.result()
print("Query results loaded to the table {}".format(table_id))