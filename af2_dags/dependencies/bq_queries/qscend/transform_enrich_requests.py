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


def build_dashburgh_street_tix_query(dataset, raw_table, new_table, is_deduped, id_field, group_field, limit,
                                     start_time, field_groups):
    sql = f"""
    CREATE OR REPLACE TABLE `{os.environ['GCLOUD_PROJECT']}.{dataset}.{new_table}` AS 
    SELECT {'DISTINCT' if is_deduped else ''} {id_field} AS id, dept, tix.request_type_name, closed_date_est
    FROM `{os.environ['GCLOUD_PROJECT']}.{dataset}.{raw_table}` tix
    INNER JOIN
        (SELECT {group_field}, COUNT(*) AS `count`
        FROM `{os.environ['GCLOUD_PROJECT']}.{dataset}.{raw_table}`
        WHERE """
    group_list = []
    for group, fields in field_groups.items():
        field_list = []
        group_str = f"{group} IN ("
        for field in fields:
            field_list.append(f"'{field}'")
        group_str += ", ".join(str(field) for field in field_list) + ")"
        group_list.append(group_str)
    sql += " AND ".join(str(group) for group in group_list)
    sql += f"""
        GROUP BY {group_field}
        ORDER BY `count` DESC
        LIMIT {limit}) top_types
    ON tix.{group_field} = top_types.{group_field}
    WHERE tix."""
    sql += " AND ".join(str(group) for group in group_list)
    sql += f"""
    AND status_name = 'closed'
    AND create_date_unix >= {start_time}
    """
    return sql


def delete_table_group(char_pattern):
    return f"""
    FOR record IN (
        SELECT CONCAT("DROP TABLE ", table_schema, ".", table_name, ";") AS del_statement
        FROM {os.environ['GCLOUD_PROJECT']}.qalert.INFORMATION_SCHEMA.TABLES
        WHERE table_name LIKE "{char_pattern}"
        ORDER BY table_name DESC
    ) DO
    EXECUTE IMMEDIATE
      FORMAT('''
          %s
      ''', record.del_statement);
    END
      FOR
    """


def document_missed_requests(backfill_table):
    return f"""
    CREATE OR REPLACE TABLE `{os.environ['GCLOUD_PROJECT']}.qalert.missed_requests` AS
        SELECT DISTINCT * FROM `{os.environ['GCLOUD_PROJECT']}.qalert.{backfill_table}`
        UNION DISTINCT
        SELECT DISTINCT * FROM `{os.environ['GCLOUD_PROJECT']}.qalert.missed_requests`
    """


def drop_pii(safe_fields, private_types, create_table=True):
    sql = ""
    if create_table:
        sql = f"CREATE OR REPLACE TABLE `{os.environ['GCLOUD_PROJECT']}.qalert.data_export_scrubbed` AS "
    sql += f"""
    SELECT
        group_id,
        TO_JSON_STRING(child_tickets) AS child_tickets,
        num_requests,
        parent_closed,
        {safe_fields}
    FROM
        `{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests`
    WHERE 
        request_type_name NOT IN ({private_types})
    """
    return sql


def format_incoming_data_types(incoming_table, cols):
    return F"""
    CREATE OR REPLACE TABLE `{os.environ['GCLOUD_PROJECT']}.qalert.{incoming_table}` AS
    WITH formatted  AS 
        (
        SELECT 
            DISTINCT * EXCEPT (create_date_est, create_date_utc, last_action_est, last_action_utc, 
                               closed_date_est, closed_date_utc, input_pii_lat, input_pii_long, google_pii_lat, 
                               google_pii_long, input_anon_lat, input_anon_long, google_anon_lat, google_anon_long),
            PARSE_DATETIME('%Y-%m-%d %X', LEFT(create_date_est, LENGTH(create_date_est)-6)) AS create_date_est,
            PARSE_TIMESTAMP('%Y-%m-%d %X%Ez', create_date_utc) create_date_utc,
            PARSE_DATETIME('%Y-%m-%d %X', LEFT(last_action_est, LENGTH(last_action_est)-6)) AS last_action_est,
            PARSE_TIMESTAMP('%Y-%m-%d %X%Ez', last_action_utc) AS last_action_utc,
            PARSE_DATETIME('%Y-%m-%d %X', LEFT(closed_date_est, LENGTH(closed_date_est)-6)) AS closed_date_est, 
            PARSE_TIMESTAMP('%Y-%m-%d %X%Ez', closed_date_utc) AS closed_date_utc, 
            CAST(input_pii_lat AS FLOAT64) AS input_pii_lat,
            CAST(input_pii_long AS FLOAT64) AS input_pii_long,
            CAST(google_pii_lat AS FLOAT64) AS google_pii_lat,
            CAST(google_pii_long AS FLOAT64) AS google_pii_long,
            CAST(input_anon_lat AS FLOAT64) AS input_anon_lat,
            CAST(input_anon_long AS FLOAT64) AS input_anon_long,
            CAST(google_anon_lat AS FLOAT64) AS google_anon_lat,
            CAST(google_anon_long AS FLOAT64) AS google_anon_long,
        FROM 
            {os.environ['GCLOUD_PROJECT']}.qalert.{incoming_table}
        )
    -- drop the final column through slicing the string (-13). final column is added in next query     
    SELECT 
        {cols} 
    FROM 
        formatted
    """


def sync_311_updates(dataset, upd_table, upd_id_field, new_table):
    return F"""
    SELECT * FROM `{os.environ['GCLOUD_PROJECT']}.{dataset}.{upd_table}` base
    WHERE base.{upd_id_field} NOT IN 
        (SELECT DISTINCT {upd_id_field} FROM `{os.environ['GCLOUD_PROJECT']}.{dataset}.{new_table}`)
    UNION ALL
    SELECT m.{upd_id_field}, alr.child_ids, alr.num_requests, alr.parent_closed, alr.status_name, alr.status_code, alr.dept, 
           m.request_type_name, m.request_type_id, m.origin, alr.pii_comments, alr.anon_comments, alr.pii_private_notes, 
           alr.create_date_est, alr.create_date_utc, alr.create_date_unix, alr.last_action_est, alr.last_action_utc, 
           alr.last_action_unix, alr.closed_date_est, alr.closed_date_utc, alr.closed_date_unix, m.pii_street_num, m.street, 
           m.cross_street, m.street_id, m.cross_street_id, m.city, m.pii_input_address, m.pii_google_formatted_address, 
           m.anon_google_formatted_address, m.address_type, m.neighborhood_name, m.council_district, m.ward, m.police_zone, 
           m.fire_zone, m.dpw_streets, m.dpw_enviro, m.dpw_parks, m.input_pii_lat, m.input_pii_long, m.google_pii_lat, 
           m.google_pii_long, m.input_anon_lat, m.input_anon_long, m.google_anon_lat, m.google_anon_long
    FROM `{os.environ['GCLOUD_PROJECT']}.{dataset}.{new_table}` m, 
         `{os.environ['GCLOUD_PROJECT']}.{dataset}.{upd_table}` alr 
    WHERE m.{upd_id_field} = alr.{upd_id_field}
    """
