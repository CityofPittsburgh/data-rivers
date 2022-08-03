from google.cloud import bigquery, storage
import os

bq_client = bigquery.Client()
storage_client = storage.Client()

table_id = f"{os.environ['GCLOUD_PROJECT']}.ceridian.employee_vs_gen_pop_gender_comp"
job_config = bigquery.QueryJobConfig(
    destination=table_id,
    use_legacy_sql=False,
    write_disposition = 'WRITE_TRUNCATE',
    create_disposition = 'CREATE_IF_NEEDED'
)

sql = F"""-- create a table with up-to-date gender demographics for the City of Pittsburgh
          -- workforce compared to the gender demographics of the overall city population
          -- based on the 2020 US Census.
FROM (
  SELECT gender, COUNT(DISTINCT(employee_num)) AS gender_count, SUM(COUNT(*)) OVER() AS total
  FROM `{os.environ['GCLOUD_PROJECT']}.ceridian.active_employees` 
  GROUP BY gender
)
UNION ALL
SELECT 'M' AS gender, 49.00 AS percentage, 'Overall City' AS type
UNION ALL
SELECT 'F' AS gender, 51.00 AS percentage, 'Overall City' AS type;
"""

query_job = bq_client.query(sql, job_config=job_config)
query_job.result()
print("Query results loaded to the table {}".format(table_id))