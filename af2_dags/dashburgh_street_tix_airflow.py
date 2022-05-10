from __future__ import absolute_import

import os

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from deprecated import airflow_utils
from deprecated.airflow_utils import get_ds_month, get_ds_year, default_args

# The goal of this mini-DAG is to perform a weekly pull of all DPW Streets Maintenance tasks
# and store it in a table, which is then accessed by a Power BI chart to be displayed in the
# Dashburgh open data project

dag = DAG(
    'dashburgh_street_tix',
    default_args=default_args,
    schedule_interval='@daily',
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year}
)

query = f"""-- create a table containing every unique ticket fulfilled by a DPW Streets Division
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

format_street_tix = BigQueryOperator(
    task_id='dashburgh_format_street_tix',
    sql=query,
    use_legacy_sql=False,
    destination_dataset_table=f"{os.environ['GCLOUD_PROJECT']}:qalert.dashburgh_street_tix",
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    dag=dag
)

qalert_beam_cleanup = BashOperator(
    task_id='dashburgh_street_tix_beam_cleanup',
    bash_command= airflow_utils.beam_cleanup_statement('{}_dashburgh_street_tix'.format(os.environ['GCS_PREFIX'])),
    dag=dag
)

format_street_tix >> qalert_beam_cleanup