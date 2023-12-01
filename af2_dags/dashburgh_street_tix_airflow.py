from __future__ import absolute_import
import os

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_month, get_ds_year, default_args
from dependencies.bq_queries.qscend import transform_enrich_requests as q

# The goal of this mini-DAG is to perform a daily pull of all DPW Streets Maintenance tasks
# and store it in a table, which is then accessed by a Power BI chart to be displayed in the
# Dashburgh open data project

dag = DAG(
    'dashburgh_street_tix',
    default_args=default_args,
    schedule_interval='@daily',
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year}
)

dataset = 'qalert'
raw_table = 'all_linked_requests'
new_table = 'dashburgh_street_tix'
is_deduped = True
id_field = 'group_id'
group_field = 'request_type_name'
# limit the query to only the top 10 request types so the Dashburgh chart isn't too cluttered
limit = 10
start_time = 1577854800
field_groups = {group_field: [
    # each of these request types are identified as DPW
    # Streets Maintenance requests by the Qalert online request portal
    'Angle Iron', 'Barricades', 'City Steps, Need Cleared',
    'Curb/Request for Asphalt Windrow', 'Drainage/Leak',
    'Graffiti, Removal', 'Leaves/Street Cleaning', 'Litter', 'Litter Can',
    'Litter Can, Public', 'Litter, Public Property', 'Overgrowth',
    'Port A Potty', 'Potholes', 'Public Right of Way', 'Salt Box',
    'Snow/Ice removal', 'Street Cleaning/Sweeping', 'Trail Maintenance',
    'Tree Fallen Across Road', 'Tree Fallen Across Sidewalk'],
    # each of these department names refer to DPW Divisions that have at one point
    # completed street maintenance tasks. 'DPW - Streets Maintenance' was introduced
    # in 2020 as a catchall department name for this type of request
    'dept': ['DPW - Division 1', 'DPW - Division 2',
             'DPW - 2nd Division', 'DPW - Division 3',
             'DPW - Division 5', 'DPW - Division 6',
             'DPW - Street Maintenance']
}

# create a table containing every unique ticket fulfilled by a DPW Streets Division
format_street_tix = BigQueryOperator(
    task_id='format_street_tix',
    sql=q.build_dashburgh_street_tix_query(dataset, raw_table, new_table, is_deduped, id_field, group_field,
                                           limit, start_time, field_groups),
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

qalert_beam_cleanup = BashOperator(
    task_id='qalert_beam_cleanup',
    bash_command=airflow_utils.beam_cleanup_statement(f"{os.environ['GCS_PREFIX']}_qalert"),
    dag=dag
)

format_street_tix >> qalert_beam_cleanup
