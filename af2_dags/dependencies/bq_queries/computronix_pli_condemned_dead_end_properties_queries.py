import os

# make a view?
# cluster by type
# partition by type?
# partition by date
# partition by ingestion time
# partition by geo?
def combine_incoming_existing_recs():
    return F"""
CREATE OR REPLACE TABLE `{os.environ['GCLOUD_PROJECT']}.computronix.pli_cde_properties` AS

WITH new_t AS
(
SELECT 
    DISTINCT *      
FROM `{os.environ['GCLOUD_PROJECT']}.computronix.incoming_pli_program_inspection_properties`
WHERE 
(parc_num IS NOT NULL )
AND 
(insp_type_desc LIKE 'Condemned Property') 
OR 
(insp_type_desc LIKE 'Dead End Property')
), 

old_t AS 
(
SELECT 
    DISTINCT old_t.* 
FROM `{os.environ['GCLOUD_PROJECT']}.computronix.pli_cde_properties` old_t
LEFT OUTER JOIN new_t ON old_t.parc_num = new_t.parc_num
WHERE new_t.parc_num IS NULL
)

SELECT DISTINCT * FROM
    (SELECT * FROM new_t 
    WHERE (parc_num IS NOT NULL)
    
    UNION ALL
    
    SELECT * FROM old_t 
    WHERE (parc_num IS NOT NULL));
"""

# tables direc from cde view?
# hard overwrite? or keep record?

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
"""


##  this is no longer needed? just export direc from cde in bridgis
def create_wprdc_exp_table():
    return F"""
CREATE OR REPLACE TABLE 
`{os.environ['GCLOUD_PROJECT']}.computronix.pli_cde_properties_wprdc_exp` AS
SELECT 
    *
FROM `{os.environ['GCLOUD_PROJECT']}.computronix.pli_cde_properties`
WHERE insp_type_desc LIKE 'Dead End Property' OR insp_type_desc LIKE 'Condemned Property'
"""

# must be a new table but can be in bridgis
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

# must be a new table but can be in bridgis
def push_gis_all_recs():
    return F"""
CREATE OR REPLACE TABLE `data-bridgis.computronix.cde_properties` AS
SELECT 
  cde.*,
FROM `{os.environ['GCLOUD_PROJECT']}.computronix.pli_cde_properties_wprdc_exp` cde
"""
