import os


def build_data_quality_table(dataset, new_table, source_table, field):
    return F"""
    CREATE OR REPLACE TABLE `{os.environ['GCLOUD_PROJECT']}.data_quality_check.{new_table}` AS 
    SELECT DISTINCT {field}
    FROM `{os.environ['GCLOUD_PROJECT']}.{dataset}.{source_table}`
    ORDER BY {field} ASC
    """
