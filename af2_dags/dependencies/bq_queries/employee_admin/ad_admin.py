import os


def active_directory_personas():
    return F"""
    CREATE OR REPLACE TABLE `{os.environ['GCLOUD_PROJECT']}.active_directory.active_directory_personas` AS
    SELECT c.employee_num AS ceridian_id, i.mpoetc_number, i.badge_number, c.first_name, 
           c.last_name, c.display_name, a.email, a.sam_account_name, c.hire_date, i.rank, 
           i.unit, c.dept_desc, c.office, c.job_title, c.status, c.ethnicity, c.gender
    FROM `{os.environ['GCLOUD_PROJECT']}.ceridian.all_employees` c
    LEFT OUTER JOIN `{os.environ['GCLOUD_PROJECT']}.intime.employee_data` i
    ON c.employee_num = i.employee_id
    LEFT OUTER JOIN `{os.environ['GCLOUD_PROJECT']}.active_directory.ad_users` a
    ON c.employee_num = a.employee_id
    """
