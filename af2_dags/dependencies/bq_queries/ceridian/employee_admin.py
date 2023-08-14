import os


def extract_new_hires():
    return F"""
    CREATE OR REPLACE TABLE `{os.environ['GCLOUD_PROJECT']}.ceridian.daily_new_hires` AS
    SELECT employee_num, display_name, dept_desc, hire_date, account_modified_date, pay_class, 
    IF(job_title LIKE '%Unpaid%', 'Unpaid', 'Paid') AS pay_status 
    FROM `{os.environ['GCLOUD_PROJECT']}.ceridian.all_employees`
    WHERE status = 'Active'
    AND PARSE_DATETIME('%Y-%m-%d', hire_date) > PARSE_DATETIME('%Y-%m-%d', account_modified_date)
    AND DATE_DIFF(CURRENT_DATETIME(), PARSE_DATETIME('%Y-%m-%d', account_modified_date), DAY) <= 1
    """
