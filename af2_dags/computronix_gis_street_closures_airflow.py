from __future__ import absolute_import

import os
import datetime
import time

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator

from dependencies import airflow_utils
from dependencies.airflow_utils import get_ds_year, get_ds_month, get_ds_day, default_args


"""
This DAG retrieves a DOMI dataset containing street closures. The API it hits doesn't have a good way (as of 10/22)  to 
pull only newly updated permits. Thus, all are pulled and this contains expired, future, and current closures. The 
entire dataset is stored in DOMI's bucket as a CSV, as well as WPRDC's bucket as a CSV, and the data-rivers BQ. 
Finally, data are also stored in data-bridGIS' BQ for publication on a connection to an ESRI server. 
"""
dag = DAG(
    'computronix_gis_street_closures',
    default_args=default_args,
    schedule_interval='@hourly',
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year, 'get_ds_day': get_ds_day},
    start_date=datetime.datetime(2022, 12, 16),
    catchup=False
)

# initialize gcs locations
bucket = F"gs://{os.environ['GCS_PREFIX']}_computronix"
dataset = "gis_domi_street_closures"
path = "{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ ds|get_ds_day }}/{{ run_id }}"
json_loc = F"{path}_street_closures.json"
avro_loc = F"avro_output/{path}/"

# Run gcs_loader
exec_gcs = F"python {os.environ['GCS_LOADER_PATH']}/computronix_gis_street_closures_gcs.py"
gcs_loader = BashOperator(
    task_id='gcs_loader',
    bash_command=f"{exec_gcs} --output_arg {dataset}/{json_loc}",
    dag=dag
)

# Run DF
exec_df = F"python {os.environ['DATAFLOW_SCRIPT_PATH']}/computronix_gis_street_closures_dataflow.py"
dataflow = BashOperator(
    task_id='dataflow',
    bash_command=F"{exec_df} --input {bucket}/{dataset}/{json_loc} --avro_output {bucket}/{dataset}/{avro_loc}",
    dag=dag
)

# join the carte_id vals to the corresponding lat/long
query_join = F"""
CREATE OR REPLACE TABLE `{os.environ["GCLOUD_PROJECT"]}.computronix.gis_street_closures_partitioned` 
PARTITION BY RANGE_BUCKET(active, GENERATE_ARRAY(0, 1)) AS
SELECT 
 sc.*,
 lalo.geometry
FROM `{os.environ["GCLOUD_PROJECT"]}.computronix.gis_street_closures` sc
JOIN `{os.environ["GCLOUD_PROJECT"]}.timebound_geography.carte_id_street_segment_coords` lalo 
ON sc.carte_id = lalo.carte_id  
"""
join_coords = BigQueryOperator(
    task_id='join_coords',
    sql=query_join,
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

# Export table (with the geometry from above) as CSV to WPRDC bucket (file name is the date. path contains the date
# info)
dest_bucket = F"gs://{os.environ['GCS_PREFIX']}_wprdc/domi_street_closures/"
wprdc_export = BigQueryToCloudStorageOperator(
    task_id='wprdc_export',
    source_project_dataset_table=F"{os.environ['GCLOUD_PROJECT']}.computronix.gis_street_closures",
    destination_cloud_storage_uris=[F"{dest_bucket}street_segments.csv"],
    bigquery_conn_id='google_cloud_default',
    dag=dag
)

# Export table as CSV to DOMI bucket (file name is the date. path contains the date info)
csv_file_name = F"{path}"
dest_bucket = F"gs://{os.environ['GCS_PREFIX']}_domi/domi_street_closures/"
domi_export = BigQueryToCloudStorageOperator(
    task_id='domi_export',
    source_project_dataset_table=F"{os.environ['GCLOUD_PROJECT']}.computronix.gis_street_closures",
    destination_cloud_storage_uris=[F"{dest_bucket}street_segments.csv"],
    bigquery_conn_id='google_cloud_default',
    dag=dag
)

# push table of all permits to data-bridGIS BQ
query_push_all = F"""
CREATE OR REPLACE TABLE `data-bridgis.computronix.gis_street_closures_partitioned` AS 
SELECT 
  * EXCEPT(from_date_UTC, from_date_EST, from_date_UNIX, to_date_UTC, to_date_EST, to_date_UNIX),
  (PARSE_DATETIME ("%m/%d/%Y %H:%M:%S",from_date_EST)) as from_EST,
  (PARSE_DATETIME ("%m/%d/%Y %H:%M:%S",to_date_EST)) as to_EST,
  (PARSE_DATETIME ("%m/%d/%Y %H:%M:%S",from_date_UTC)) as from_UTC,
  (PARSE_DATETIME ("%m/%d/%Y %H:%M:%S",to_date_UTC)) as to_UTC
FROM `{os.environ["GCLOUD_PROJECT"]}.computronix.gis_street_closures_partitioned` 	
"""
push_data_bridgis = BigQueryOperator(
    task_id='push_data_bridgis',
    sql=query_push_all,
    bigquery_conn_id='google_cloud_default',
    use_legacy_sql=False,
    dag=dag
)

beam_cleanup = BashOperator(
    task_id='beam_cleanup',
    bash_command=airflow_utils.beam_cleanup_statement(F"{os.environ['GCS_PREFIX']}_computronix"),
    dag=dag
)

# branching DAG splits after the reverse geo coding  and converges back at beam_cleanup
gcs_loader >> dataflow >> join_coords
join_coords >> wprdc_export >> beam_cleanup
join_coords >> domi_export >> beam_cleanup
join_coords >> push_data_bridgis >> beam_cleanup
