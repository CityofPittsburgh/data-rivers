from google.cloud import bigquery, storage
import os

bq_client = bigquery.Client()
storage_client = storage.Client()

job_config = bigquery.QueryJobConfig(use_legacy_sql=False)

sql = f"""-- update all_linked_requests with the data stored in the temporary mismatch table
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

query_job = bq_client.query(sql, job_config=job_config)
query_job.result()
print("Query update complete")