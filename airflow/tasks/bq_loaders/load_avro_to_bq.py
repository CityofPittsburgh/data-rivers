from google.cloud import bigquery


def load_avro_to_bq(dataset, table, gcs_bucket):
    bq_client = bigquery.Client()
    dataset_id = dataset
    table = table
    dataset_ref = bq_client.dataset(dataset_id)
    uri = 'gs://{}}/avro_output/avro_output*'.format(gcs_bucket)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.AVRO

    load_job = bq_client.load_table_from_uri(
        uri, dataset_ref.table(table), job_config=job_config
    )

    load_job.result()
