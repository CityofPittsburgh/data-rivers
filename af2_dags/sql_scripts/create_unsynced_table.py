from google.cloud import bigquery, storage
import os

bq_client = bigquery.Client()
storage_client = storage.Client()

table_id = f"{os.environ['GCLOUD_PROJECT']}.qalert.temp_curr_status_merge"
job_config = bigquery.QueryJobConfig(
    destination=table_id,
    use_legacy_sql=False,
    write_disposition = 'WRITE_TRUNCATE',
    create_disposition = 'CREATE_IF_NEEDED'
)

sql = F"""-- create a temporary table with the most up-to-date request types and
          -- geographic data sourced from all_tickets_current_status where the 
          -- request type, origin, and geo data does not match that found in all_linked_requests
SELECT DISTINCT group_id, req_types.request_type_name, req_types.request_type_id, req_types.origin,
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
WHERE IFNULL(alr.request_type_name, "") != IFNULL(req_types.request_type_name, "")
OR IFNULL(alr.origin, "") != IFNULL(req_types.origin, "")
OR IFNULL(alr.address_type, "") != IFNULL(geos.address_type, "")
OR IFNULL(alr.neighborhood_name, "") != IFNULL(geos.neighborhood_name, "")
OR IFNULL(alr.council_district, "") != IFNULL(geos.council_district, "")
OR IFNULL(alr.ward, "") != IFNULL(geos.ward, "")
OR IFNULL(alr.police_zone, "") != IFNULL(geos.police_zone, "")
OR IFNULL(alr.fire_zone, "") != IFNULL(geos.fire_zone, "")
OR IFNULL(alr.dpw_streets, "") != IFNULL(geos.dpw_streets, "")
OR IFNULL(alr.dpw_enviro, "") != IFNULL(geos.dpw_enviro, "")
OR IFNULL(alr.dpw_parks, "") != IFNULL(geos.dpw_parks, "")
OR alr.within_city != geos.within_city
"""

query_job = bq_client.query(sql, job_config=job_config)
query_job.result()
print("Query results loaded to the table {}".format(table_id))