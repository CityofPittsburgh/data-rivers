import os


def build_ad_personas_table():
    return F"""
    CREATE OR REPLACE TABLE `{os.environ['GCLOUD_PROJECT']}.active_directory.active_directory_personas` AS
    SELECT c.employee_num AS ceridian_id, i.mpoetc_number, i.badge_number, c.first_name, 
           c.last_name, c.display_name, a.email, a.sam_account_name, c.hire_date, i.rank, 
           i.unit, c.dept_desc, c.office, c.job_title, c.status, c.ethnicity, c.gender, i.web_rms_dropdown
    FROM `{os.environ['GCLOUD_PROJECT']}.ceridian.all_employees` c
    LEFT OUTER JOIN `{os.environ['GCLOUD_PROJECT']}.intime.employee_data` i
    ON c.employee_num = i.employee_id
    LEFT OUTER JOIN `{os.environ['GCLOUD_PROJECT']}.active_directory.ad_users` a
    ON c.employee_num = a.employee_id
    """


def enhance_ad_table():
    return F"""
    CREATE OR REPLACE TABLE `{os.environ['GCLOUD_PROJECT']}.active_directory.ad_users` AS
    SELECT * FROM (
        SELECT * FROM `{os.environ['GCLOUD_PROJECT']}.active_directory.ad_users_raw`
        WHERE email NOT IN (SELECT email FROM `{os.environ['GCLOUD_PROJECT']}.active_directory.ad_ceridian_matches`)
        UNION ALL
        SELECT employee_num, first_name, last_name, email, sam_account_name title, department, enabled
        FROM `{os.environ['GCLOUD_PROJECT']}.active_directory.ad_ceridian_matches`
    ) ORDER BY last_name ASC
    """


def update_ids_from_ceridian(new_table, where_clause=''):
    return F"""
    CREATE OR REPLACE TABLE `{os.environ['GCLOUD_PROJECT']}.active_directory.{new_table}` AS
    SELECT * FROM (
        WITH sso_match AS (
          WITH id_fix AS (
            SELECT * FROM `{os.environ['GCLOUD_PROJECT']}.active_directory.ad_users_raw`
            WHERE employee_id IS NULL OR SAFE_CAST(employee_id AS INT64) IS NULL
          )
          SELECT DISTINCT c.employee_num, id_fix.first_name, id_fix.last_name, id_fix.email, 
                 id_fix.sam_account_name, id_fix.title, id_fix.department,
                 CASE 
                    WHEN c.employee_num IS NOT NULL THEN c.office
                    ELSE id_fix.description
                 END AS description, id_fix.enabled, c.status AS ceridian_status
          FROM id_fix
          LEFT JOIN `{os.environ['GCLOUD_PROJECT']}.ceridian.all_employees` c
          ON LOWER(id_fix.email) = LOWER(c.sso_login)
        )
        SELECT DISTINCT 
               CASE WHEN sso_match.employee_num IS NOT NULL THEN sso_match.employee_num 
               ELSE c.employee_num END AS employee_num, 
               sso_match.first_name, sso_match.last_name, sso_match.email, 
               sso_match.sam_account_name, sso_match.title, sso_match.department,
               CASE 
                 WHEN sso_match.employee_num IS NULL AND c.employee_num IS NOT NULL THEN c.office
                 ELSE sso_match.description
               END AS description, sso_match.enabled, IFNULL(sso_match.ceridian_status, c.status) AS ceridian_status
        FROM sso_match
        LEFT JOIN `{os.environ['GCLOUD_PROJECT']}.ceridian.all_employees` c
        ON LOWER(sso_match.first_name) = LOWER(c.first_name)
           AND LOWER(sso_match.last_name) = LOWER(c.last_name)
           AND sso_match.department = c.dept_desc
    ) WHERE employee_num IS {where_clause} NULL
    """
