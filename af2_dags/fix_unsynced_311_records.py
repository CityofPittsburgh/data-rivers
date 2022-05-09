from __future__ import absolute_import

import os

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_month, get_ds_year, default_args

# The goal of this mini-DAG is to fix a recurring issue where 311 ticket data differs
# between their records in all_tickets_current_status and all_linked_requests in BigQuery

dag = DAG(
    'fix_unsynced_311_records',
    default_args=default_args,
    schedule_interval='@daily',
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year}
)

create_query = f"""-- create a temporary table with the most up-to-date request types and
                   -- geographic DATA sourced from all_tickets_current_status where the 
                   -- request type and geo data does not match that found in all_linked_requests
        SELECT group_id, req_types.request_type_name, req_types.request_type_id, req_types.origin,
               geos.pii_street_num, geos.street, geos.cross_street, geos.street_id, geos.cross_street_id,
               geos.city, geos.pii_input_address, geos.pii_google_formatted_address, geos.address_type, 
               geos.anon_google_formatted_address, geos.neighborhood_name, geos.council_district, geos.ward,
               geos.police_zone, geos.fire_zone, geos.dpw_streets, geos.dpw_enviro, geos.dpw_parks,
               geos.pii_lat, geos.pii_long, geos.anon_lat, geos.anon_long, geos.within_city
        FROM `{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests` alr
        INNER JOIN
        (SELECT DISTINCT id, request_type_name, request_type_id, origin
        FROM `{os.environ['GCLOUD_PROJECT']}.qalert.all_tickets_current_status`) req_types
        ON alr.group_id = req_types.id
        INNER JOIN
        (SELECT DISTINCT id, pii_street_num, street, cross_street, street_id, cross_street_id,
                city, pii_input_address, pii_google_formatted_address, anon_google_formatted_address,
                address_type, neighborhood_name, council_district, ward, police_zone, fire_zone,
                dpw_streets, dpw_enviro, dpw_parks, pii_lat, pii_long, anon_lat, anon_long, within_city
        FROM `{os.environ['GCLOUD_PROJECT']}.qalert.all_tickets_current_status`) geos
        ON alr.group_id = geos.id
        WHERE IFNULL(alr.request_type_name, "") != req_types.request_type_name
        OR IFNULL(alr.origin, "") != req_types.origin
        OR IFNULL(alr.address_type, "") != geos.address_type 
        OR IFNULL(alr.neighborhood_name, "") != geos.neighborhood_name
        OR IFNULL(alr.council_district, "") != geos.council_district 
        OR IFNULL(alr.ward, "") != IFNULL(geos.ward, "")
        OR IFNULL(alr.police_zone, "") != geos.police_zone
        OR IFNULL(alr.fire_zone, "") != geos.fire_zone
        OR IFNULL(alr.dpw_streets, "") != geos.dpw_streets 
        OR IFNULL(alr.dpw_enviro, "") != geos.dpw_enviro
        OR IFNULL(alr.dpw_parks, "") != geos.dpw_parks 
        OR alr.within_city != geos.within_city
        """

create_mismatch_table = BigQueryOperator(
    task_id='create_mismatch_table',
    sql=create_query,
    use_legacy_sql=False,
    destination_dataset_table=f"{os.environ['GCLOUD_PROJECT']}:qalert.temp_curr_status_merge",
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    dag=dag
)

upd_query = f"""-- update all_linked_requests with the data stored in the temporary mismatch table
            UPDATE `{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests` alr
            SET alr.request_type_name = temp.request_type_name, alr.request_type_id = temp.request_type_id,
            alr.pii_street_num = temp.pii_street_num, alr.street = temp.street, 
            alr.cross_street = temp.cross_street, alr.street_id = temp.street_id, 
            alr.cross_street_id = temp.cross_street_id, alr.city = temp.city, 
            alr.pii_input_address = temp.pii_input_address, 
            alr.pii_google_formatted_address = temp.pii_google_formatted_address, 
            alr.origin = temp.origin, alr.address_type = temp.address_type, 
            alr.anon_google_formatted_address = temp.anon_google_formatted_address, 
            alr.neighborhood_name = temp.neighborhood_name, 
            alr.council_district = temp.council_district, alr.ward = temp.ward,
            alr.police_zone = temp.police_zone, alr.fire_zone = temp.fire_zone, 
            alr.dpw_streets = temp.dpw_streets, alr.dpw_enviro = temp.dpw_enviro, 
            alr.dpw_parks = temp.dpw_parks, alr.pii_lat = temp.pii_lat,
            alr.pii_long = temp.pii_long, alr.anon_lat = temp.anon_lat,
            alr.anon_long = temp.anon_long, alr.within_city = temp.within_city
            FROM `{os.environ['GCLOUD_PROJECT']}.qalert.temp_curr_status_merge` temp
            WHERE alr.group_id = temp.group_id
            """

fix_ticket_mismatches = BigQueryOperator(
    task_id='fix_ticket_mismatches',
    sql=upd_query,
    use_legacy_sql=False,
    dag=dag
)

beam_cleanup = BashOperator(
    task_id='beam_cleanup',
    bash_command=airflow_utils.beam_cleanup_statement('{}_ticket_mismatch_fix'.format(os.environ['GCS_PREFIX'])),
    dag=dag
)

create_mismatch_table >> fix_ticket_mismatches >> beam_cleanup