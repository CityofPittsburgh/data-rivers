import logging

from google.cloud import bigquery, storage
from scourgify import normalize_address_record


bq_client = bigquery.Client()
storage_client = storage.Client()


def load_avro_to_bq(dataset, table, gcs_bucket, date_partition=False, partition_by=None):
    bq_client = bigquery.Client()
    dataset_id = dataset
    table = table
    dataset_ref = bq_client.dataset(dataset_id)
    uri = 'gs://{}}/avro_output/avro_output*'.format(gcs_bucket)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.AVRO
    if date_partition == True:
        job_config.time_partitioning = bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.DAY,
                                                                 field=partition_by)

    load_job = bq_client.load_table_from_uri(
        uri, dataset_ref.table(table), job_config=job_config
    )

    load_job.result()
    print 'Data loaded to {} table, {} dataset'.format(table, dataset)


def cleanup_beam_avro(bucket_name):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)

    avro = bucket.blob('avro_output/*')
    avro.delete()
    print 'Avro files deleted'

    beam_temp = bucket.blob('temp/*')
    beam_temp.delete()
    print 'Beam temp files deleted'

    beam_staging = bucket.blob('staging/*')
    beam_staging.delete()
    print 'Beam staging files deleted'


def geocode_address(address):
    query = """
    SELECT
      geometry
    FROM
      geography.pittsburgh_addresses AS addresses
    WHERE
      normalized_address = {}
    LIMIT
        1 
    """.format(address)
    bq_client = bigquery.Client()
    job_config = bigquery.QueryJobConfig()
    query_job = bq_client.query(query)

    result = query_job.result()
    point = result[0]
    return point

def latlong_to_point(lat, long):
    return 'ST_GEOGPOINT({}, {})'.format(long, lat)


def reverse_geocode_latlong(lat, long):
    bq_client = bigquery.Client()
    #TODO: figure out how to create temp table, not regular, and then access temp table in subsequent query
    temp_table_query = """
    CREATE OR REPLACE TABLE geography.point
    (point GEOGRAPHY);
    INSERT INTO geography.point
    VALUES(ST_GEOGPOINT({}{}{}));
    """.format(long, ',', lat)

    temp_query = bq_client.query(temp_table_query)
    result = temp_query.result()

    rev_geo_query = """
        SELECT
          point.*,
          neighborhoods.hood AS neighborhood,
          council_districts.council_district,
          wards.ward,
          fire_zones.firezones AS fire_zone,
          dpw_divisions.objectid AS dpw_division
        FROM
          geography.point AS point
        JOIN
          geography.neighborhoods
        ON
          ST_CONTAINS(neighborhoods.geometry, point.point)
        JOIN
          geography.council_districts
        ON
          ST_CONTAINS(council_districts.geometry, point.point)
        JOIN
          geography.wards
        ON
          ST_CONTAINS(wards.geometry, point.point)
        JOIN
          geography.fire_zones
        ON
          ST_CONTAINS(fire_zones.geometry, point.point)
        JOIN
          geography.dpw_divisions
        ON
          ST_CONTAINS(dpw_divisions.geometry, point.point)
    """
    query_job = bq_client.query(rev_geo_query)
    result = query_job.result()
    data = {}
    for row in result:
        data['neighborhood'] = row.neighborhood
        data['fire_zone'] = row.fire_zone
        # TODO: fix problem with council_district geography table: change district values from string to int
        data['council_district'] = int(row.council_district)
        data['dpw_division'] = row.dpw_division
        data['ward'] = row.ward
    print result
    return data


def load_avro_to_bq(dataset, table, gcs_bucket):
    bq_client = bigquery.Client()
    dataset_id = dataset
    table = table
    dataset_ref = bq_client.dataset(dataset_id)
    uri = 'gs://{}}/avro_output/avro_output*'.format(gcs_bucket)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.AVRO

    load_job = bq_client.load_table_from_uri(
        uri, dataset_ref.table(table), job_config=job_config
    )

    load_job.result()

# def reverse_geocode_point(point):

#TODO: fix formatting so run() does not throw error
# if __name__ == '__main__':
#     logging.getLogger().setLevel(logging.INFO)
#     run()
