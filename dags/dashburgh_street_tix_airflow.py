from __future__ import absolute_import

import os

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_month, get_ds_year, default_args

dag = DAG(
    'dashburgh_street_tix',
    default_args=default_args,
    schedule_interval='@daily',
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year}
)

query = """SELECT DISTINCT id, dept, requestType, closedOn
             FROM `{}.qalert.requests`
            WHERE requestType IN (
                  'Angle Iron', 'Barricades', 'City Steps, Need Cleared', 
                  'Curb/Request for Asphalt Windrow', 'Drainage/Leak', 'Graffiti, Removal', 
                  'Leaves/Street Cleaning', 'Litter', 'Litter Can', 
                  'Litter Can, Public', 'Litter, Public Property', 'Overgrowth', 
                  'Port A Potty', 'Potholes', 'Public Right of Way', 
                  'Salt Box', 'Snow/Ice removal', 'Street Cleaning/Sweeping',
                  'Trail Maintenance', 'Tree Fallen Across Road', 'Tree Fallen Across Sidewalk'
                  )
            AND status = 'closed'
            AND dept  IN ('DPW - Division 1', 'DPW - Division 2', 'DPW - 2nd Division',
                          'DPW - Division 3', 'DPW - Division 5', 'DPW - Division 6', 
                          'DPW - Street Maintenance'
                          )
            AND createDateUnix >= 1577854800""".format(os.environ['GCLOUD_PROJECT'])

format_street_tix = BigQueryOperator(
    task_id='dashburgh_format_street_tix',
    sql=query,
    use_legacy_sql=False,
    destination_dataset_table='{}:qalert.dashburgh_street_tix'.format(os.environ['GCLOUD_PROJECT']),
    write_disposition='WRITE_APPEND',
    time_partitioning={'type': 'DAY'},
    dag=dag
)

format_street_tix