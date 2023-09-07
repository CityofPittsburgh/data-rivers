import os


def build_city_limits_query(raw_table, lat_field='lat', long_field='long'):
    """
    Determine whether a set of coordinates fall within the borders of the City of Pittsburgh,
    while also falling outside the borders of Mt. Oliver. If an address is within the city,
    the address_type field is left as-is. Otherwise, address_type is changed to 'Outside
    of City'.
    :param raw_table: name of the table containing raw 311 data
    :param lat_field: name of table column that contains latitude value
    :param long_field: name of table column that contains longitude value
    :return: string to be passed through as arg to BigQueryOperator
    **NOTE**: A strange issue occurs with the Mt Oliver borders if it is stored in BigQuery in GEOGRAPHY format.
    All lat/longs outside of the Mt Oliver boundaries are identified as inside Mt Oliver when passed through ST_COVERS,
    and all lat/longs inside of Mt Oliver are identified as outside of it. To get around this problem, we have stored
    the boundary polygons as strings, and then convert those strings to polygons using ST_GEOGFROMTEXT.
    """

    return f"""
    UPDATE `{os.environ['GCLOUD_PROJECT']}.qalert.{raw_table}`
    SET address_type = IF ( 
       ((ST_COVERS((ST_GEOGFROMTEXT((SELECT geometry FROM `{os.environ['GCLOUD_PROJECT']}.timebound_geography.pittsburgh_and_mt_oliver_borders`
                                      WHERE zone = 'Mt. Oliver'))),
               ST_GEOGPOINT(`{os.environ['GCLOUD_PROJECT']}.qalert.{raw_table}`.{long_field},
                    `{os.environ['GCLOUD_PROJECT']}.qalert.{raw_table}`.{lat_field})))
        OR NOT 
        ST_COVERS((ST_GEOGFROMTEXT((SELECT geometry FROM `{os.environ['GCLOUD_PROJECT']}.timebound_geography.pittsburgh_and_mt_oliver_borders`
                                     WHERE zone = 'Pittsburgh'))),
                   ST_GEOGPOINT(`{os.environ['GCLOUD_PROJECT']}.qalert.{raw_table}`.{long_field}, 
                   `{os.environ['GCLOUD_PROJECT']}.qalert.{raw_table}`.{lat_field}))
       ), 'Outside of City', address_type )
    WHERE `{os.environ['GCLOUD_PROJECT']}.qalert.{raw_table}`.{long_field} IS NOT NULL AND 
    `{os.environ['GCLOUD_PROJECT']}.qalert.{raw_table}`.{lat_field} IS NOT NULL
    """


def delete_table_group(char_pattern):
    return f"""
    FOR record IN (
        SELECT CONCAT("DROP TABLE ", table_schema, ".", table_name, ";") AS del_statement
        FROM {os.environ['GCLOUD_PROJECT']}.qalert.INFORMATION_SCHEMA.TABLES
        WHERE table_name LIKE "{char_pattern}"
        ORDER BY table_name DESC
    ) DO
    EXECUTE IMMEDIATE
      FORMAT(%s, record.del_statement);
    END
      FOR
    """

def drop_pii(safe_fields, private_types):
    return f"""
    CREATE OR REPLACE TABLE `{os.environ['GCLOUD_PROJECT']}.qalert.data_export_scrubbed` AS
    SELECT
        group_id,
        child_ids,
        num_requests,
        parent_closed,
        {safe_fields}
    FROM
        `{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests`
    WHERE 
        request_type_name NOT IN ({private_types})
    """
