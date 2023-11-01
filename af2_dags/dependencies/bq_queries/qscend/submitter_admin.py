import os


def join_submitter_to_request():
    return F"""
    CREATE OR REPLACE TABLE `{os.environ['GCLOUD_PROJECT']}.qalert.311_utilizers` AS
    SELECT request_id, create_date_est, request_type_name, pii_google_formatted_address AS complaint_address, 
           submitter_id, first_name, last_name, email, phone, address, address_2, ls.city, state, zip, 
           neighborhood_name, twitter_name, last_request_date, last_modified_date, curr_total_requests_made, 
           curr_total_requests_closed, satisfaction_level, origin
    FROM `{os.environ['GCLOUD_PROJECT']}.qalert.latest_submitters` ls
    LEFT OUTER JOIN `{os.environ['GCLOUD_PROJECT']}.qalert.all_tickets_current_status` atcs
    ON ls.request_id = atcs.id
    WHERE CONCAT(request_id,submitter_id) NOT IN (SELECT CONCAT(request_id,submitter_id)
                                          FROM `{os.environ['GCLOUD_PROJECT']}.qalert.311_utilizers`)
    UNION ALL
    SELECT request_id, create_date_est, request_type_name, complaint_address, 
           submitter_id, first_name, last_name, email, phone, address, address_2, city, state, zip, 
           neighborhood_name, twitter_name, last_request_date, last_modified_date, curr_total_requests_made, 
           curr_total_requests_closed, satisfaction_level, origin 
    FROM `{os.environ['GCLOUD_PROJECT']}.qalert.ticket_submitter_summaries` 
    """


def update_submitter_table():
    return F"""
    CREATE OR REPLACE TABLE `{os.environ['GCLOUD_PROJECT']}.qalert.311_utilizers` AS
    SELECT DISTINCT submitter_id, first_name, last_name, email, phone, address, address_2, 
                    city, state, zip, twitter_name, MAX(curr_total_requests_made) AS total_requests_made, 
                    MAX(curr_total_requests_closed) AS total_requests_closed
    FROM `{os.environ['GCLOUD_PROJECT']}.qalert.latest_submitters`
    GROUP BY submitter_id, first_name, last_name, email, phone, address, address_2, city, state, zip, twitter_name
    UNION ALL
    SELECT DISTINCT submitter_id, first_name, last_name, email, phone, address, address_2, 
                    city, state, zip, twitter_name, total_requests_made, total_requests_closed
    FROM `{os.environ['GCLOUD_PROJECT']}.qalert.311_utilizers`
    WHERE submitter_id NOT IN (SELECT submitter_id FROM `{os.environ['GCLOUD_PROJECT']}.qalert.latest_submitters`)
    """
