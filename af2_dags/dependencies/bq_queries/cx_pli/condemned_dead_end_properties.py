import os

# this will combine and dedupe all the incoming condemned and dead end properties with the existing ones
def combine_incoming_existing_recs():
    return F"""
CREATE OR REPLACE TABLE `{os.environ['GCLOUD_PROJECT']}.computronix.pli_cde_properties` AS

-- Select all the incoming records (these records are not limited to condemned or dead end properties)
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

-- Select all the previously ingested records that are not included in the new records (these are missing from the 
-- API request due to an error etc)
missing_rec_t AS 
(
SELECT 
    DISTINCT old_t.* 
FROM `{os.environ['GCLOUD_PROJECT']}.computronix.pli_cde_properties` old_t
LEFT OUTER JOIN new_t ON old_t.parc_num = new_t.parc_num
WHERE new_t.parc_num IS NULL
)

-- Union together all the incoming records (these are the latest updates) with any records that weren't in the 
-- incoming results (these were recrods that didn't get includeed in the API request (due to an error etc)). Since the 
-- extraction of records from the API contains the entire table of all CDE properties, missing records can only be an 
-- error. Since API call will terminate at an error and return partial results, this deals with the problem of missing 
-- data (there isn't currently an option for change data capture)
SELECT DISTINCT * FROM
    (SELECT * FROM new_t 
    WHERE (parc_num IS NOT NULL)
    
    UNION ALL
    
    SELECT * FROM missing_rec_t 
    WHERE (parc_num IS NOT NULL));
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
WHERE (insp_type_desc LIKE 'Condemned Property' AND insp_status LIKE 'Active')
"""


def push_gis_latest_updates():
    return F"""
    CREATE OR REPLACE TABLE `data-bridgis.computronix.cde_properties_latest_update_active` AS
    SELECT 
      cde.*,
      ROW_NUMBER () OVER (ORDER BY create_date_UNIX) as parc_unique_id
    FROM `{os.environ['GCLOUD_PROJECT']}.computronix.pli_cde_properties` cde
    JOIN
    (SELECT 
      MAX(create_date_unix) as max_date,
      parc_num
    FROM `{os.environ['GCLOUD_PROJECT']}.computronix.pli_cde_properties`
    GROUP BY parc_num
    ) AS max_vals
    ON cde.create_date_UNIX = max_vals.max_date AND cde.parc_num = max_vals.parc_num
    
    WHERE insp_status LIKE 'Active'
    
    ORDER BY cde.create_date_UNIX
    """
