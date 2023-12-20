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


def build_format_dedup_query(dataset, fmt_table, src_table, cast_fields, cols_in_order, datestring_fmt="", tz=""):
    sql = f"""
    CREATE OR REPLACE TABLE `{os.environ['GCLOUD_PROJECT']}.{dataset}.{fmt_table}` AS
    WITH formatted  AS 
        (
        SELECT DISTINCT * EXCEPT ("""
    sql += ", ".join(str(field['field']) for field in cast_fields)
    sql += "), "
    cast_str_list = []
    for conv in cast_fields:
        if conv['type'] in ('DATE', 'DATETIME'):
            cast_str_list.append(f'PARSE_{conv["type"]}("{datestring_fmt}", {conv["field"]}) AS {conv["field"]}')
        elif conv['type'] == 'TIMESTAMP':
            cast_str_list.append(f'PARSE_TIMESTAMP("{datestring_fmt}", {conv["field"]}, "{tz}") AS {conv["field"]}')
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


def build_sync_staging_table_query(dataset, new_table, upd_table, src_table, is_deduped, upd_id_field, join_id_field,
                                   field_groups, comp_fields):
    sql = F"""
    CREATE OR REPLACE TABLE `{os.environ['GCLOUD_PROJECT']}.{dataset}.{new_table}` AS 
    SELECT {'DISTINCT' if is_deduped else ''} {upd_id_field}, """
    field_list = []
    for group in field_groups:
        for alias, fields in group.items():
            for field in fields:
                field_list.append(f"{alias}.{field}")
    sql += ", ".join(str(field) for field in field_list)
    sql += f" FROM `{os.environ['GCLOUD_PROJECT']}.{dataset}.{upd_table}` upd"
    for group in field_groups:
        for alias, fields in group.items():
            join_list = []
            sql += f" INNER JOIN (SELECT {'DISTINCT' if is_deduped else ''} {join_id_field}, "
            for field in fields:
                join_list.append(f"{field}")
            sql += ", ".join(str(field) for field in join_list)
            sql += f""" FROM `{os.environ['GCLOUD_PROJECT']}.{dataset}.{src_table}`) {alias}
            ON upd.{upd_id_field} = {alias}.{join_id_field}"""
    sql += " WHERE "
    comparison_list = []
    for group in comp_fields:
        for alias, fields in group.items():
            for field in fields:
                comparison_list.append(f'IFNULL(upd.{field}, "") != IFNULL({alias}.{field}, "") ')
    sql += "OR ".join(str(field) for field in comparison_list)
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


def dedup_table(dataset, table):
    return f"""
    SELECT DISTINCT * FROM `{os.environ['GCLOUD_PROJECT']}.{dataset}.{table}`
    """


def direct_gcs_export(export_uri, file_type, fields, query_string, delim=','):
    return f"""
    EXPORT DATA OPTIONS (
        uri='{export_uri}*.{file_type}',
        format='{file_type}',
        overwrite=true,
        header=true,
        field_delimiter='{delim}'
    )
    AS SELECT DISTINCT {fields}
    FROM ({query_string})
    """


def filter_old_values(dataset, temp_table, final_table, join_field):
    return f"""
    DELETE FROM `{os.environ['GCLOUD_PROJECT']}.{dataset}.{final_table}` final
    WHERE final.{join_field} IN (SELECT {join_field} FROM `{os.environ['GCLOUD_PROJECT']}.{dataset}.{temp_table}`)
    """


def update_time_balances_table(dataset, master_table, temp_table):
    return F"""
    CREATE OR REPLACE TABLE `{os.environ['GCLOUD_PROJECT']}.{dataset}.{master_table}` AS
    SELECT DISTINCT employee_id, PARSE_DATE('%Y-%m-%d', `date`) AS retrieval_date, time_bank, code, balance
    FROM `{os.environ['GCLOUD_PROJECT']}.{dataset}.{temp_table}`
    UNION ALL
    SELECT DISTINCT employee_id, retrieval_date, time_bank, code, balance
    FROM `{os.environ['GCLOUD_PROJECT']}.{dataset}.{master_table}`
    WHERE CONCAT(employee_id, ':', CAST(retrieval_date AS STRING)) NOT IN (
        SELECT CONCAT(employee_id, ':', `date`)
        FROM `{os.environ['GCLOUD_PROJECT']}.{dataset}.{temp_table}`
    )
    """
