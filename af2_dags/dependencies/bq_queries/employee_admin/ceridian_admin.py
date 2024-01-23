import os
from datetime import datetime, timedelta


def build_eeo4_report():
    return F"""
    CREATE OR REPLACE TABLE `{os.environ['GCLOUD_PROJECT']}.ceridian.equal_opportunity_report` AS
    SELECT job_function, salary_range, ethnicity, gender, COUNT(*) counts FROM (
    SELECT j.job_function, CASE
        WHEN base_salary <= 15999.89 THEN '$0.1 - $15.9'
        WHEN base_salary BETWEEN 16000.00 AND 19999.99 THEN '$16.0 - $19.9'
        WHEN base_salary BETWEEN 20000.00 AND 24999.99 THEN '$20.0 - $24.9'
        WHEN base_salary BETWEEN 25000.00 AND 32999.99 THEN '$25.0 - $32.9'
        WHEN base_salary BETWEEN 33000.00 AND 42999.99 THEN '$33.0 - $42.9'
        WHEN base_salary BETWEEN 43000.00 AND 54999.99 THEN '$43.0 - $54.9'
        WHEN base_salary BETWEEN 55000.00 AND 69999.99 THEN '$55.0 - $69.9'
        WHEN base_salary >= 70000.00 THEN '$70.0 PLUS'
        ELSE 'Unknown Range'
    END AS salary_range, ethnicity, gender
    FROM `{os.environ['GCLOUD_PROJECT']}.ceridian.job_details` j
    RIGHT OUTER JOIN (SELECT job_title, base_salary, ethnicity, gender
    FROM `{os.environ['GCLOUD_PROJECT']}.ceridian.all_employees`
    WHERE dept_desc NOT IN ('Non-Employee Benefits', 'Historical')
    AND job_title != 'Community Liaison') e_1
    ON j.job_title = e_1.job_title
    UNION ALL
    SELECT 'Officials and Administrators' AS job_function, CASE
        WHEN base_salary <= 15999.89 THEN '$0.1 - $15.9'
        WHEN base_salary BETWEEN 16000.00 AND 19999.99 THEN '$16.0 - $19.9'
        WHEN base_salary BETWEEN 20000.00 AND 24999.99 THEN '$20.0 - $24.9'
        WHEN base_salary BETWEEN 25000.00 AND 32999.99 THEN '$25.0 - $32.9'
        WHEN base_salary BETWEEN 33000.00 AND 42999.99 THEN '$33.0 - $42.9'
        WHEN base_salary BETWEEN 43000.00 AND 54999.99 THEN '$43.0 - $54.9'
        WHEN base_salary BETWEEN 55000.00 AND 69999.99 THEN '$55.0 - $69.9'
        WHEN base_salary >= 70000.00 THEN '$70.0 PLUS'
        ELSE 'Unknown Range'
    END AS salary_range, ethnicity, gender
    FROM (SELECT job_title, base_salary, ethnicity, gender
    FROM `{os.environ['GCLOUD_PROJECT']}.ceridian.all_employees`
    WHERE job_title = 'Community Liaison' AND dept_desc = 'Office of the Mayor')
    UNION ALL
    SELECT 'Professionals' AS job_function, CASE
        WHEN base_salary <= 15999.89 THEN '$0.1 - $15.9'
        WHEN base_salary BETWEEN 16000.00 AND 19999.99 THEN '$16.0 - $19.9'
        WHEN base_salary BETWEEN 20000.00 AND 24999.99 THEN '$20.0 - $24.9'
        WHEN base_salary BETWEEN 25000.00 AND 32999.99 THEN '$25.0 - $32.9'
        WHEN base_salary BETWEEN 33000.00 AND 42999.99 THEN '$33.0 - $42.9'
        WHEN base_salary BETWEEN 43000.00 AND 54999.99 THEN '$43.0 - $54.9'
        WHEN base_salary BETWEEN 55000.00 AND 69999.99 THEN '$55.0 - $69.9'
        WHEN base_salary >= 70000.00 THEN '$70.0 PLUS'
        ELSE 'Unknown Range'
    END AS salary_range, ethnicity, gender
    FROM (SELECT job_title, base_salary, ethnicity, gender
    FROM `{os.environ['GCLOUD_PROJECT']}.ceridian.all_employees`
    WHERE job_title = 'Community Liaison' AND dept_desc != 'Office of the Mayor'))
    GROUP BY job_function, salary_range, ethnicity, gender
    """


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


def compare_timebank_balances(comp_table, offset=0):
    query = F"""
    SELECT c.employee_id, e.display_name, c.retrieval_date, i.code, 
           c.balance AS ceridian_balance, i.balance AS intime_balance
    FROM `{os.environ['GCLOUD_PROJECT']}.ceridian.historic_accrual_balances` c, 
         `{os.environ['GCLOUD_PROJECT']}.intime.timebank_balances` i,
         `{os.environ['GCLOUD_PROJECT']}.ceridian.all_employees` e
    WHERE c.employee_id = i.employee_id AND c.employee_id = e.employee_num
    AND c.time_bank = i.time_bank
    """

    today = datetime.today()
    offset_val = timedelta(days=offset)
    comp_date = today + offset_val
    comp_str = comp_date.strftime("%m/%d/%Y")

    if comp_table == 'balance_update':
        query += f"""AND c.retrieval_date = PARSE_DATE('%m/%d/%Y', '{comp_str}') 
                     AND i.retrieval_date = PARSE_DATE('%m/%d/%Y', '{comp_str}')
                     AND c.code IN ('Military', 'COVAC', 'PPL')
                     AND i.code IN ('Military', 'COVAC', 'PPL')"""

    elif comp_table == 'discrepancy_report':
        query += f"""AND c.retrieval_date = PARSE_DATE('%m/%d/%Y', '{today.strftime('%m/%d/%Y')}') 
                     AND i.retrieval_date = PARSE_DATE('%m/%d/%Y', '{comp_str}')"""

    query += " AND ROUND(c.balance, 1) != ROUND(i.balance, 1)"
    return query


def extract_employee_manager_info():
    return F"""
    SELECT e.display_name, e.sso_login AS email, e.dept_desc, e.manager_name, m.sso_login AS manager_email, e.status
    FROM `{os.environ['GCLOUD_PROJECT']}.ceridian.all_employees` e
    LEFT OUTER JOIN (SELECT display_name, sso_login FROM `{os.environ['GCLOUD_PROJECT']}.ceridian.all_employees`) m
    ON e.manager_name = m.display_name
    WHERE e.status IN ('Active', 'Pre-Start')
    """


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


def extract_recent_terminations():
    return F"""
    SELECT employee_num, sso_login, first_name, last_name, dept_desc, status, termination_date, pay_class
    FROM `{os.environ['GCLOUD_PROJECT']}.ceridian.all_employees`
    WHERE status = 'Terminated' AND 
    DATE_DIFF(CURRENT_DATETIME(), PARSE_DATETIME('%Y-%m-%d', termination_date), DAY) <= 30
    """


def pmo_export_query():
    return F"""
    SELECT employee_num, first_name, last_name, sso_login, dept_desc, office, 
           job_title, hire_date, `union`, manager_name, status
    FROM `{os.environ['GCLOUD_PROJECT']}.ceridian.all_employees`
    WHERE status IN ('Active', 'Pre-Start')
    AND dept_desc NOT IN ('Bureau of Police', 'Bureau of Emergency Medical Services', 
                          'Bureau of Fire', 'Bureau of School Crossing Guards')
    """


def update_time_accruals_table():
    return F"""
    CREATE OR REPLACE TABLE `{os.environ['GCLOUD_PROJECT']}.ceridian.historic_accrual_balances` AS
    SELECT DISTINCT employee_id, PARSE_DATE('%Y-%m-%d', `date`) AS retrieval_date, time_bank, code, balance
    FROM `{os.environ['GCLOUD_PROJECT']}.ceridian.time_accruals_report`
    UNION ALL
    SELECT DISTINCT employee_id, retrieval_date, time_bank, code, balance
    FROM `{os.environ['GCLOUD_PROJECT']}.ceridian.historic_accrual_balances`
    WHERE CONCAT(employee_id, ':', CAST(retrieval_date AS STRING)) NOT IN (
        SELECT CONCAT(employee_id, ':', `date`)
        FROM `{os.environ['GCLOUD_PROJECT']}.ceridian.time_accruals_report`
    )
    """
