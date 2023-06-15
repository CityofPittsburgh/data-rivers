import os


def combine_incoming_existing_recs():
    return F"""
CREATE OR REPLACE TABLE `{os.environ['GCLOUD_PROJECT']}.computronix.pli_cde_properties` AS
WITH new_t AS
(SELECT 
    *      
FROM `{os.environ['GCLOUD_PROJECT']}.computronix.incoming_pli_program_inspection_properties`
),

old_t AS 
(SELECT 
    old_t.* 
FROM `{os.environ['GCLOUD_PROJECT']}.computronix.pli_cde_properties` old_t
LEFT OUTER JOIN new_t ON old_t.parc_num = new_t.parc_num
WHERE new_t.parc_num IS NULL
)

SELECT * FROM new_tp
UNION ALL
SELECT * FROM old_t;
"""


def create_pli_exp_active_tables():
    return F"""
CREATE OR REPLACE TABLE `{os.environ['GCLOUD_PROJECT']}.computronix.pli_active_dead_end_properties` AS
SELECT 
    *      
FROM `{os.environ['GCLOUD_PROJECT']}.computronix.pli_cde_properties`
WHERE (insp_type_desc LIKE 'Dead End Property' AND insp_status LIKE 'Active');


CREATE OR REPLACE TABLE `{os.environ['GCLOUD_PROJECT']}.computronix.pli_active_condemned_properties` AS
SELECT 
    *      
FROM `{os.environ['GCLOUD_PROJECT']}.computronix.pli_cde_properties`
WHERE (insp_type_desc LIKE 'Condemned' AND insp_status LIKE 'Active')
,"""


def create_wprdc_exp_table():
    return F"""
CREATE OR REPLACE TABLE 
`{os.environ['GCLOUD_PROJECT']}.computronix.pli_cde_properties_wprdc_exp` AS
SELECT 
    *
FROM `{os.environ['GCLOUD_PROJECT']}.computronix.pli_cde_properties`
WHERE insp_type_desc LIKE 'Dead End Property' OR insp_type_desc LIKE 'Condemned Property'
"""


def push_gis_latest_updates():
    return F"""
    CREATE OR REPLACE TABLE `data-bridgis.computronix.cde_properties_latest_update` AS
    SELECT 
      cde.*,
      ROW_NUMBER () OVER (ORDER BY create_date_UNIX) as parc_unique_id
    FROM `{os.environ['GCLOUD_PROJECT']}.computronix.pli_cde_properties_wprdc_exp` cde
    JOIN
    (SELECT 
      MAX(create_date_unix) as max_date,
      parc_num
    FROM `{os.environ['GCLOUD_PROJECT']}.computronix.pli_cde_properties_wprdc_exp`
    GROUP BY parc_num
    ) AS max_vals
    ON cde.create_date_UNIX = max_vals.max_date AND cde.parc_num = max_vals.parc_num
    ORDER BY cde.create_date_UNIX
    """


def push_gis_all_recs():
    return F"""
CREATE OR REPLACE TABLE `data-bridgis.computronix.cde_properties` AS
SELECT 
  cde.*,
FROM `{os.environ['GCLOUD_PROJECT']}.computronix.pli_cde_properties_wprdc_exp` cde""
"""
