import os


def build_data_quality_table(dataset, new_table, source_table, field):
    return F"""
    CREATE OR REPLACE TABLE `{os.environ['GCLOUD_PROJECT']}.data_quality_check.{new_table}` AS 
    SELECT DISTINCT {field}
    FROM `{os.environ['GCLOUD_PROJECT']}.{dataset}.{source_table}`
    WHERE {field} IS NOT NULL
    ORDER BY {field} ASC
    """


def build_dedup_old_updates(dataset, table, id_field, last_upd_field):
    sql = F"""
    CREATE OR REPLACE TABLE `{os.environ['GCLOUD_PROJECT']}.{dataset}.{table}` AS
    SELECT * EXCEPT (rn)
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER(PARTITION BY {id_field} ORDER BY {last_upd_field} DESC) AS rn
        FROM `{os.environ['GCLOUD_PROJECT']}.{dataset}.{table}`
    )
    WHERE rn = 1;
    """
    return sql


def build_format_dedup_query(dataset, fmt_table, src_table, cast_fields, cols_in_order, datestring_fmt=""):
    sql = f"""
    CREATE OR REPLACE TABLE `{os.environ['GCLOUD_PROJECT']}.{dataset}.{fmt_table}` AS
    WITH formatted  AS 
        (
        SELECT DISTINCT * EXCEPT ("""
    sql += ", ".join(str(field['field']) for field in cast_fields)
    sql += "), "
    cast_str_list = []
    for conv in cast_fields:
        if conv['type'] == 'DATETIME':
            cast_str_list.append(f'PARSE_DATETIME("{datestring_fmt}", {conv["field"]}) AS {conv["field"]}')
        else:
            cast_str_list.append(f'CAST({conv["field"]} AS {conv["type"]}) AS {conv["field"]}')
    sql += ", ".join(str(cast) for cast in cast_str_list)
    sql += f"""
    FROM 
        {os.environ['GCLOUD_PROJECT']}.{dataset}.{src_table}
    )
    SELECT 
        {cols_in_order} 
    FROM 
        formatted
    """
    return sql


def build_geo_coords_from_parcel_query(raw_table, parc_field, lat_field="latitude", long_field="longitude"):
    return F"""
    SELECT
        raw.*,
        ST_Y(ST_CENTROID(p.geometry)) AS {lat_field}, 
        ST_X(ST_CENTROID(p.geometry)) AS {long_field}
    FROM `{raw_table}` raw
    LEFT OUTER JOIN `{os.environ['GCLOUD_PROJECT']}.timebound_geography.parcels` p ON 
    {parc_field} = p.zone
    """


def build_insert_new_records_query(dataset, incoming_table, master_table, id_field, cols):
    sql = F"""
    INSERT INTO `{os.environ['GCLOUD_PROJECT']}.{dataset}.{master_table}` ({cols})
    (
        SELECT {cols} FROM `{os.environ['GCLOUD_PROJECT']}.{dataset}.{incoming_table}` inc
        WHERE inc.{id_field} NOT IN (
            SELECT mst.{id_field} 
            FROM `{os.environ['GCLOUD_PROJECT']}.{dataset}.{master_table}` mst
        )
    )
    """
    return sql


def build_sync_update_query(dataset, upd_table, src_table, id_field, upd_fields):
    sql = f"UPDATE `{os.environ['GCLOUD_PROJECT']}.{dataset}.{upd_table}` upd SET "
    upd_str_list = []
    for field in upd_fields:
        upd_str_list.append(f"upd.{field} = temp.{field}")
    sql += ", ".join(str(upd) for upd in upd_str_list)
    sql += f"""
    FROM `{os.environ['GCLOUD_PROJECT']}.{dataset}.{src_table}` temp
    WHERE upd.{id_field} = temp.{id_field}
    """
    return sql
