import os


def build_revgeo_time_bound_query(dataset, source, create_date, lat_field, long_field,
                                  new_table=None, source_is_table=True):
    """
    Take a table with lat/long values and reverse-geocode it into a new a final table.
    This function is a substantial refactor of the build_rev_geo() function. This query allows a lat/long point to be
    localized to a zone within a specifc time range. This is critical as some boundaries have changed over time.

    Since boundaries change over time, this query builds upon a previous version, by selecting the boundaries at a
    relevant time point.


    :param dataset: BigQuery dataset (string)
    :param source: starting dataset that has not been rev geocoded yet. this should be either 1) the name of the
    table containing the source data, or 2) the name of the alias from the common table expression containing the
    source data. Note that all project/dataset information is added  in the query string and only the table/CTE alias
    name are needed (string)
    :param new_table: name of table that will hold the fully geocoded datta (string)
    :param create_date: field in raw_table that contains the creation date (string)
    :param lat_field: field in table that identifies latitude value (string)
    :param long_field: field in table that identifies longitude value (string)
    :param source_is_table: indicates that the source dataset (which will be rev geocoded) is an existing
    table (default = true). if this is false, the datasource can be derived from a common table expression (value =
    false). this allows this operation to be bundled together into a larger query without staging table creation.
    This function was always used on existing tables prior to  9/23 and the functions requires no modifications in
    input arguments to maintain this usage because it defaults to True (boolean).

    :return: string to be passed through as arg to BigQueryOperator
    """
    if source_is_table:
        src = F"`{os.environ['GCLOUD_PROJECT']}.{dataset}.{source}`"
        create_statement = F"CREATE OR REPLACE TABLE `{os.environ['GCLOUD_PROJECT']}.{dataset}.{new_table}` AS"
    else:
        src = source
        create_statement = ""

    return f"""
    {create_statement}
    -- return zones for all records that it is possible to rev geocode. some records will not be possible to process 
    -- (bad lat/long etc) and will be pulled in via the next blocked
    SELECT
        source.*,
        CAST (t_hoods.zone AS STRING) AS neighborhood_name,
        CAST (t_cd.zone AS STRING) AS council_district,
        CAST (t_w.zone AS STRING) AS ward,
        CAST (t_fz.zone AS STRING) AS fire_zone,
        CAST (t_pz.zone AS STRING) AS police_zone,
        CAST (t_st.zone AS STRING) AS dpw_streets,
        CAST (t_es.zone AS STRING) AS dpw_enviro,
        CAST (t_pk.zone AS STRING) AS dpw_parks
      FROM
        {src} source

      -- neighborhoods
      LEFT OUTER JOIN `{os.environ["GCLOUD_PROJECT"]}.timebound_geography.neighborhoods` AS t_hoods ON
        ST_CONTAINS(t_hoods.geometry, ST_GEOGPOINT(source.{long_field}, source.{lat_field}))
        AND TIMESTAMP(PARSE_DATETIME('%m/%d/%Y %H:%M:%S-00:00',t_hoods.start_date)) <= TIMESTAMP(source.{create_date})
        AND IFNULL(TIMESTAMP(PARSE_DATETIME('%m/%d/%Y %H:%M:%S-00:00', t_hoods.end_date)),
            CURRENT_TIMESTAMP()) >= TIMESTAMP(source.{create_date})


      -- council districts
      LEFT OUTER JOIN `{os.environ["GCLOUD_PROJECT"]}.timebound_geography.council_districts` AS t_cd ON
        ST_CONTAINS(t_cd.geometry, ST_GEOGPOINT(source.{long_field}, source.{lat_field}))
        AND TIMESTAMP(PARSE_DATETIME('%m/%d/%Y %H:%M:%S-00:00',t_cd.start_date)) <= TIMESTAMP(source.{create_date})
        AND IFNULL(TIMESTAMP(PARSE_DATETIME('%m/%d/%Y %H:%M:%S-00:00', t_cd.end_date)),
            CURRENT_TIMESTAMP()) >= TIMESTAMP(source.{create_date})

      -- wards
      LEFT OUTER JOIN `{os.environ["GCLOUD_PROJECT"]}.timebound_geography.wards` AS t_w ON
        ST_CONTAINS(t_w.geometry, ST_GEOGPOINT(source.{long_field}, source.{lat_field}))
        AND TIMESTAMP(PARSE_DATETIME('%m/%d/%Y %H:%M:%S-00:00',t_w.start_date)) <= TIMESTAMP(source.{create_date})
        AND IFNULL(TIMESTAMP(PARSE_DATETIME('%m/%d/%Y %H:%M:%S-00:00', t_w.end_date)),
            CURRENT_TIMESTAMP()) >= TIMESTAMP(source.{create_date})

      -- fire zones
      LEFT OUTER JOIN `{os.environ["GCLOUD_PROJECT"]}.timebound_geography.fire_zones` AS t_fz ON
         ST_CONTAINS(t_fz.geometry, ST_GEOGPOINT(source.{long_field}, source.{lat_field}))
        AND TIMESTAMP(PARSE_DATETIME('%m/%d/%Y %H:%M:%S-00:00',t_fz.start_date)) <= TIMESTAMP(source.{create_date})
        AND IFNULL(TIMESTAMP(PARSE_DATETIME('%m/%d/%Y %H:%M:%S-00:00', t_fz.end_date)),
            CURRENT_TIMESTAMP()) >= TIMESTAMP(source.{create_date})

      -- police zones
      LEFT OUTER JOIN `{os.environ["GCLOUD_PROJECT"]}.timebound_geography.police_zones` AS t_pz ON
         ST_CONTAINS(t_pz.geometry, ST_GEOGPOINT(source.{long_field}, source.{lat_field}))
        AND TIMESTAMP(PARSE_DATETIME('%m/%d/%Y %H:%M:%S-00:00',t_pz.start_date)) <= TIMESTAMP(source.{create_date})
        AND IFNULL(TIMESTAMP(PARSE_DATETIME('%m/%d/%Y %H:%M:%S-00:00', t_pz.end_date)),
            CURRENT_TIMESTAMP()) >= TIMESTAMP(source.{create_date})

      -- DPW streets division
      LEFT OUTER JOIN `{os.environ["GCLOUD_PROJECT"]}.timebound_geography.dpw_streets_divisions` AS t_st ON
       ST_CONTAINS(t_st.geometry, ST_GEOGPOINT(source.{long_field}, source.{lat_field}))
        AND TIMESTAMP(PARSE_DATETIME('%m/%d/%Y %H:%M:%S-00:00',t_st.start_date)) <= TIMESTAMP(source.{create_date})
        AND IFNULL(TIMESTAMP(PARSE_DATETIME('%m/%d/%Y %H:%M:%S-00:00', t_st.end_date)),
            CURRENT_TIMESTAMP()) >= TIMESTAMP(source.{create_date})

      -- DPW environment services division
      LEFT OUTER JOIN `{os.environ["GCLOUD_PROJECT"]}.timebound_geography.dpw_es_divisions` AS t_es ON 
         ST_CONTAINS(t_es.geometry, ST_GEOGPOINT(source.{long_field}, source.{lat_field}))
        AND TIMESTAMP(PARSE_DATETIME('%m/%d/%Y %H:%M:%S-00:00',t_es.start_date)) <= TIMESTAMP(source.{create_date})
        AND IFNULL(TIMESTAMP(PARSE_DATETIME('%m/%d/%Y %H:%M:%S-00:00', t_es.end_date)),
            CURRENT_TIMESTAMP()) >= TIMESTAMP(source.{create_date})

      -- DPW parks division
      LEFT OUTER JOIN `{os.environ["GCLOUD_PROJECT"]}.timebound_geography.dpw_parks_divisions` AS t_pk ON
         ST_CONTAINS(t_pk.geometry, ST_GEOGPOINT(source.{long_field}, source.{lat_field}))
        AND TIMESTAMP(PARSE_DATETIME('%m/%d/%Y %H:%M:%S-00:00',t_pk.start_date)) <= TIMESTAMP(source.{create_date})
        AND IFNULL(TIMESTAMP(PARSE_DATETIME('%m/%d/%Y %H:%M:%S-00:00', t_pk.end_date)),
            CURRENT_TIMESTAMP()) >= TIMESTAMP(source.{create_date})
"""


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
