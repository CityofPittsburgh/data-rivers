import os


def build_revgeo_time_bound_query(dataset, source, create_date, lat_field, long_field, new_table=None):
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
    :param out_is_table:



    indicates that .

    :return: string to be passed through as arg to BigQueryOperator
    """

    if new_table:
        create_statement = F"CREATE OR REPLACE TABLE {new_table} AS"
    else:
        print("""query does not create a new table. this is idealized for embedding this function call as output in a
              second function call""")
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
        {source} source

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
