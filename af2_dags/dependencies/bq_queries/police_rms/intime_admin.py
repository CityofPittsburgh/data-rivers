import os


def extract_current_intime_details():
    return F"""
    CREATE OR REPLACE TABLE `{os.environ['GCLOUD_PROJECT']}.intime.pbp_current_assignments` AS
    SELECT * FROM (
        SELECT e.employee_id AS ceridian_id, e.mpoetc_number, e.mpoetc_username, e.ncic_username,
               e.first_name, e.last_name, e.display_name, e.email, e.hire_date, e.rank, a.permanent_rank, 
               a.activity_name AS current_activity, a.scheduled_start_time, a.scheduled_end_time, 
               e.unit AS permanent_unit, a.unit AS current_unit
        FROM `{os.environ['GCLOUD_PROJECT']}.intime.employee_data` e 
        LEFT OUTER JOIN `{os.environ['GCLOUD_PROJECT']}.intime.incoming_assignments` a
        ON e.employee_id = a.employee_id)
    WHERE current_activity IS NOT NULL
    AND (CURRENT_DATETIME('America/New_York')
        BETWEEN scheduled_start_time AND scheduled_end_time)
    """
