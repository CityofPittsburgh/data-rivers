import os
from google.cloud import bigquery

credentials = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
client = bigquery.Client.from_service_account_json(credentials)

# Get Col Names
col_names_query = """
                    SELECT 
                        column_name 
                    FROM 
                        `data-rivers-testing.dummy_qalert`.INFORMATION_SCHEMA.COLUMNS 
                    WHERE table_name LIKE "merged"
                  """
col_names_result = (client.query(col_names_query))

# Extract the Col Names from Query Result
column_names = [x['column_name'] for x in col_names_result if x['column_name'] not in ["id", "parent_ticket"]]

# Build case statement used to extract missing info from child tickets (parent tickets in t1, child tickets in t2)
case_statement = "\n".join(
        [f'CASE WHEN t1.{column_name} IS NULL THEN t2.{column_name} ELSE t1.{column_name} END AS {column_name},'
         for column_name in column_names])

"""
QUERY 1:  This query creates a view of all parent tickets with missing data copied from child tickets
          The view also contains the child tickets. After merging, the child and parent tickets appears 
          as duplicates (to be dropped later)
"""
query_1 = f'CREATE OR REPLACE VIEW data-rivers-testing.dummy_qalert.temp_merge AS\
                   SELECT t1.id,\
                   t1.parent_ticket,\
                   {case_statement}\
                   FROM data-rivers-testing.dummy_qalert.merged t1\
                   LEFT OUTER JOIN data-rivers-testing.dummy_qalert.new_children t2\
                   ON t2.parent_ticket = t1.id\
                   ORDER BY id;'

"""
QUERY 2: Return all fields from a join of the temp view (v1) with v2 which contains the number of linkages
         This join returns duplicated ticket entries which will need to be dropped in outer statement.

        We Count the number of linkages based on rows with identical IDs. The left join pulls in the number of linkages
        and also returns a duplicated field of the IDs (IDD) and count of the number of linkages (v2)
"""
query_2 = f'CREATE OR REPLACE VIEW data-rivers-testing.dummy_qalert.temp_linkages AS\
                   SELECT *\
                   FROM data-rivers-testing.dummy_qalert.temp_merge v1\
                   LEFT OUTER JOIN\
                        (\
                         SELECT id AS IDD,\
                         COUNT(id) AS num_linkages\
                         FROM data-rivers-testing.dummy_qalert.temp_merge\
                         GROUP BY IDD\
                        ) v2\
                   ON v1.id = v2.IDD;'

"""
QUERY 3: Return all fields and keep only the first row for each partitioned ID (drop occurs in WHERE clause)

         Return all fields and partition by ID (which refers to the parent ticket)
         Keeping only the first row of the duplicated IDs requires partitioning
"""
query_3 = f'CREATE OR REPLACE VIEW data-rivers-testing.dummy_qalert.temp_dedupe AS \
          SELECT * FROM\
            (\
              SELECT id,\
              parent_ticket,\
              {",".join(column_names)}\
              , num_linkages,\
              ROW_NUMBER() OVER (PARTITION BY id) AS ROWNUMBER\
              FROM data-rivers-testing.dummy_qalert.temp_linkages\
            ) a\
            WHERE ROWNUMBER = 1\
            ORDER BY a.id'

# Need to add this query
query_4 = f''

query_5 = """
            TRUNCATE `data-rivers-testing.dummy_qalert.temp_merged`;
            TRUNCATE `data-rivers-testing.dummy_qalert.temp_linkage`;
            TRUNCATE `data-rivers-testing.dummy_qalert.temp_dedupe`;
          """

for query in [query_1, query_2, query_3, query_4, query_5]:
    result = client.query(query)
