import os


def build_percentage_table_query(new_table, pct_field, hardcoded_vals):
    sql = f"""
    CREATE OR REPLACE TABLE  `{os.environ['GCLOUD_PROJECT']}.ceridian.{new_table}` AS
    SELECT {pct_field}, 
           ({pct_field}_count / total) AS percentage, 
           'City Employee' AS type
    FROM (
      SELECT {pct_field}, COUNT(DISTINCT(employee_num)) AS {pct_field}_count, SUM(COUNT(*)) OVER() AS total
      FROM `{os.environ['GCLOUD_PROJECT']}.ceridian.all_employees` 
      WHERE status = 'Active'
      GROUP BY {pct_field}
    )
    """
    for record in hardcoded_vals:
        sql += f"""
        UNION ALL
        SELECT '{record[pct_field]}' AS {pct_field}, {record['percentage']} AS percentage, 'Overall City' AS type
        """
    sql += " ORDER BY type, percentage DESC "
    return sql


def extract_new_hires():
    return F"""
    CREATE OR REPLACE TABLE `{os.environ['GCLOUD_PROJECT']}.ceridian.daily_new_hires` AS
        SELECT employee_num, first_name, last_name, display_name, sso_login, job_title, 
        manager_name, dept_desc, hire_date, account_modified_date, pay_class, 
        IF(job_title LIKE '%Unpaid%', 'Unpaid', 'Paid') AS pay_status, status AS employment_status
        FROM `{os.environ['GCLOUD_PROJECT']}.ceridian.all_employees`
        WHERE status = 'Pre-Start'
        OR status = 'Active' AND 
        (
            (PARSE_DATETIME('%Y-%m-%d', hire_date) > PARSE_DATETIME('%Y-%m-%d', account_modified_date) AND
            DATE_DIFF(CURRENT_DATETIME(), PARSE_DATETIME('%Y-%m-%d', account_modified_date), DAY) <= 1)
            OR
            DATE_DIFF(CURRENT_DATETIME(), PARSE_DATETIME('%Y-%m-%d', hire_date), DAY) <= 1
            OR
            (DATE_DIFF(CURRENT_DATETIME(), PARSE_DATETIME('%Y-%m-%d', hire_date), DAY) <= 14 AND sso_login IS NULL)
        )
        ORDER BY employment_status DESC
    """


def pmo_export_query():
    return F"""
    CREATE OR REPLACE TABLE `{os.environ['GCLOUD_PROJECT']}.ceridian.active_non_ps_employees` AS
        SELECT employee_num, first_name, last_name, sso_login, dept_desc, office, 
               job_title, hire_date, `union`, manager_name, status
        FROM `{os.environ['GCLOUD_PROJECT']}.ceridian.all_employees`
        WHERE status IN ('Active', 'Pre-Start')
        AND dept_desc NOT IN ('Bureau of Police', 'Bureau of Emergency Medical Services', 
                              'Bureau of Fire', 'Bureau of School Crossing Guards')
    """
