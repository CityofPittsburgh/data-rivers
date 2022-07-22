from google.cloud import bigquery, storage
import os

bq_client = bigquery.Client()
storage_client = storage.Client()

table_id = f"{os.environ['GCLOUD_PROJECT']}.qalert.dashburgh_street_tix"
job_config = bigquery.QueryJobConfig(
    destination=table_id,
    use_legacy_sql=False,
    write_disposition = 'WRITE_TRUNCATE',
    create_disposition = 'CREATE_IF_NEEDED'
)

sql = F"""-- create a table containing every unique ticket fulfilled by a DPW Streets Division
        SELECT DISTINCT group_id AS id, dept, tix.request_type_name, closed_date_est
        FROM `{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests` tix
        INNER JOIN
            (SELECT request_type_name, COUNT(*) AS `count`
            FROM `{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests`
            WHERE request_type_name  IN (
            -- Each of these request types are identified as DPW Streets Maintenance requests
            -- by the qalert online request portal
                'Angle Iron', 'Barricades', 'City Steps, Need Cleared', 
                'Curb/Request for Asphalt Windrow', 'Drainage/Leak', 'Graffiti, Removal', 
                'Leaves/Street Cleaning', 'Litter', 'Litter Can', 
                'Litter Can, Public', 'Litter, Public Property', 'Overgrowth', 
                'Port A Potty', 'Potholes', 'Public Right of Way', 
                'Salt Box', 'Snow/Ice removal', 'Street Cleaning/Sweeping',
                'Trail Maintenance', 'Tree Fallen Across Road', 'Tree Fallen Across Sidewalk'
            )
            -- Each of these department names refer to DPW Divisions that have at one point
            -- completed street maintenance tasks. 'DPW - Streets Maintenance' was introduced
            -- in 2020 as a catchall department name for this type of request
            AND dept IN ('DPW - Division 1', 'DPW - Division 2', 'DPW - 2nd Division',
                        'DPW - Division 3', 'DPW - Division 5', 'DPW - Division 6', 
                        'DPW - Street Maintenance'
                        )
            GROUP BY request_type_name
            ORDER BY `count` DESC
            -- limit the query to only the top 10 request types so the Dashburgh chart isn't too cluttered
            LIMIT 10) top_types
        ON tix.request_type_name = top_types.request_type_name
        WHERE tix.request_type_name IN (
            'Angle Iron', 'Barricades', 'City Steps, Need Cleared', 
            'Curb/Request for Asphalt Windrow', 'Drainage/Leak', 'Graffiti, Removal', 
            'Leaves/Street Cleaning', 'Litter', 'Litter Can', 
            'Litter Can, Public', 'Litter, Public Property', 'Overgrowth', 
            'Port A Potty', 'Potholes', 'Public Right of Way', 
            'Salt Box', 'Snow/Ice removal', 'Street Cleaning/Sweeping',
            'Trail Maintenance', 'Tree Fallen Across Road', 'Tree Fallen Across Sidewalk'
            )
        AND status_name = 'closed'
        AND dept IN ('DPW - Division 1', 'DPW - Division 2', 'DPW - 2nd Division',
                    'DPW - Division 3', 'DPW - Division 5', 'DPW - Division 6', 
                    'DPW - Street Maintenance'
                    )
        AND create_date_unix >= 1577854800"""

query_job = bq_client.query(sql, job_config=job_config)
query_job.result()
print("Query results loaded to the table {}".format(table_id))