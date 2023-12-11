import os


def extract_current_intime_details():
    return F"""
    CREATE OR REPLACE TABLE `{os.environ['GCLOUD_PROJECT']}.intime.pbp_current_assignments` AS
    SELECT * FROM (
        SELECT e.employee_id AS ceridian_id, e.mpoetc_number, e.mpoetc_username, e.ncic_username,
               e.first_name, e.last_name, e.display_name, e.email, e.hire_date, a.permanent_rank, 
               CASE 
                    WHEN a.activity_name LIKE 'Acting%' THEN a.activity_name
                    WHEN a.activity_name = 'Desk Officer' THEN a.activity_name
                    WHEN IFNULL(a.activity_name, '') != sub.activity_name AND sub.activity_name LIKE 'Acting%' THEN sub.activity_name
                    ELSE permanent_rank
               END AS current_rank, a.activity_name AS current_activity,
               a.scheduled_start_time, a.scheduled_end_time,
               sub.assignment_id AS sub_assignment_id, sub.activity_name AS sub_activity,
               sub.scheduled_start_time AS sub_activity_start_time, sub.scheduled_end_time AS sub_activity_end_time, 
               e.unit AS permanent_unit, a.unit AS current_unit, a.sub_location_name, e.web_rms_dropdown
        FROM `{os.environ['GCLOUD_PROJECT']}.intime.employee_data` e 
        LEFT OUTER JOIN `{os.environ['GCLOUD_PROJECT']}.intime.incoming_assignments` a
        ON e.employee_id = a.employee_id
        LEFT OUTER JOIN (SELECT assignment_id, parent_assignment_id, activity_name, 
                         scheduled_start_time, scheduled_end_time
                         FROM `{os.environ['GCLOUD_PROJECT']}.intime.incoming_assignments` sub
                         WHERE sub_assignment = True) sub
        ON a.assignment_id = sub.parent_assignment_id)
    WHERE (current_activity IS NOT NULL OR sub_activity IS NOT NULL)
    AND (CURRENT_TIMESTAMP() BETWEEN scheduled_start_time AND scheduled_end_time)
    """


def update_timebank_table():
    return F"""
    CREATE OR REPLACE TABLE `{os.environ['GCLOUD_PROJECT']}.intime.timebank_balances` AS
    SELECT DISTINCT employee_id, PARSE_DATE('%Y-%m-%d', `date`) AS retrieval_date, time_bank, code, balance
    FROM `{os.environ['GCLOUD_PROJECT']}.intime.current_time_balances`
    UNION ALL
    SELECT DISTINCT employee_id, retrieval_date, time_bank, code, balance
    FROM `{os.environ['GCLOUD_PROJECT']}.intime.timebank_balances`
    WHERE CONCAT(employee_id, ':', CAST(retrieval_date AS STRING)) NOT IN (
        SELECT CONCAT(employee_id, ':', `date`)
        FROM `{os.environ['GCLOUD_PROJECT']}.intime.current_time_balances`
    )
    """
