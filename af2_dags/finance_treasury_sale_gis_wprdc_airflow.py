from __future__ import absolute_import

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator

from dependencies.airflow_utils import get_ds_month, get_ds_year, default_args, \
    build_revgeo_time_bound_query, build_geo_coords_from_parcel_query

# This DAG will perform an extract and transformation of treasury saLe properties from the Real Estate Oracle
# database. Once the data is extracted, it will be uploaded to BigQuery and geocoded by matching on parcel number.
# The final output will be stored as a JSON file in GCS and made available to WPRDC for public display.


dag = DAG(
    'finance_treasury_sale_properties',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2023, 9, 18),
    catchup=False,
    user_defined_filters={'get_ds_month': get_ds_month, 'get_ds_year': get_ds_year},
    max_active_runs=1
)


data_name = "treasury_sale_properties"
path = "{{ ds|get_ds_year }}/{{ ds|get_ds_month }}/{{ run_id }}"
json_loc = f"{data_name}/{path}_treasury_sale_properties.json"


extract = BashOperator(
    task_id='extract',
    bash_command=f"python {os.environ['GCS_LOADER_PATH']}/finance_treasury_sale_gis_wprdc_extract.py "
                 f"--output_arg {json_loc}",
    dag=dag
)


# the primary key of city owned properties is parcel ID; parcel data is also stored in the timebound_geography
# dataset with corresponding geographical boundaries. this query uses the ST_CENTROID geographic function to obtain
# lat/longs for each parcel. The output of that operation is stored as lat/long and then the geocoords are used to
# reverse geocode various zonal boundariese. This is all packaged together into 1 query/operator. 

sub_query_coords_from_parc = F"""(

{build_geo_coords_from_parcel_query(
        raw_table = F"{os.environ['GCLOUD_PROJECT']}.finance.incoming_treas_sale", parc_field = "parc_num")}
        
)"""

query_geo =  build_revgeo_time_bound_query(dataset='finance', source=F"{sub_query_coords_from_parc}",
                                           new_table='geo_enriched_treas_sale_properties',
                                           create_date='treas_sale_date', lat_field='latitude', long_field='longitude',
                                           source_is_table = False)
query_full = F"""
CREATE OR REPLACE TABLE `{os.environ['GCLOUD_PROJECT']}.finance.geo_enriched_treas_sale_properties` AS
{query_geo}
"""

geo_operations = BigQueryOperator(
        task_id = 'geo_operations',
        sql = query_full,
        bigquery_conn_id='google_cloud_default',
        use_legacy_sql = False,
        dag = dag
)

# Export table as CSV to WPRDC bucket
# file name is the date. path contains the date info
csv_file_name = f"{path}"
dest_bucket = f"gs://{os.environ['GCS_PREFIX']}_wprdc/finance/treasury_sale_properties/"
wprdc_export = BigQueryToCloudStorageOperator(
        task_id = 'wprdc_export',
        source_project_dataset_table = F"{os.environ['GCLOUD_PROJECT']}.finance.geo_enriched_treas_sale_properties",
        destination_cloud_storage_uris = [F"{dest_bucket}{csv_file_name}.csv"],
        dag = dag
)

# push table to data-bridGIS BQ
query_push_gis = F"""
CREATE OR REPLACE TABLE `data-bridgis.finance.treas_sale_properties` AS
SELECT 
* 
FROM 
  `{os.environ['GCLOUD_PROJECT']}.finance.geo_enriched_treas_sale_properties`;
"""
push_gis = BigQueryOperator(
        task_id = 'push_gis',
        sql = query_push_gis,
        bigquery_conn_id = 'google_cloud_default',
        use_legacy_sql = False,
        dag = dag
)


extract >> geo_operations
geo_operations >> wprdc_export
geo_operations >> push_gis