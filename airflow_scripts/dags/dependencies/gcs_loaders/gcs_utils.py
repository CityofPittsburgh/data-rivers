from __future__ import print_function

import os
import logging
import time
import re
import argparse
import ciso8601
import ndjson

from google.cloud import storage, dlp_v2

parser = argparse.ArgumentParser()

storage_client = storage.Client()
dlp = dlp_v2.DlpServiceClient()
project = os.environ['GCLOUD_PROJECT']


def scrub_pii(field, data_objects):
    """You could reasonably make a case for doing this in the Dataflow portion of the DAG, but IMHO it's better to
    catch PII before it even gets to Cloud Storage; if we filter it at the Dataflow stage it won't make it to BigQuery,
    but will still be in GCS -- james 2/6/20"""
    for object in data_objects:
        # make sure comments field isn't empty; otherwise DLP API throws an error
        if object[field].strip(' '):
            object[field] = get_dlp_redaction(object[field])
        # google's DLP API has a rate limit of 600 requests/minute
        # TODO: consider a different workaround here; not robust for large datasets
        if data_objects.index(object) % 600 == 0 and data_objects.index(object) != 0:
            time.sleep(61)

    return data_objects


def get_dlp_redaction(uncleaned_string):
    # remove newline delimiter
    uncleaned_string = uncleaned_string.replace('\n', ' ')
    parent = dlp.project_path(project)

    # Construct inspect configuration dictionary
    info_types = ["EMAIL_ADDRESS", "FIRST_NAME", "LAST_NAME", "PHONE_NUMBER", "URL", "STREET_ADDRESS"]
    inspect_config = {"info_types": [{"name": info_type} for info_type in info_types]}

    # Construct deidentify configuration dictionary
    deidentify_config = {
        "info_type_transformations": {
            "transformations": [
                {
                    "primitive_transformation": {
                        "character_mask_config": {
                            "masking_character": "#",
                            "number_to_mask": 0,
                        }
                    }
                }
            ]
        }
    }

    # Construct item
    item = {"value": uncleaned_string}

    # Call the API
    response = dlp.deidentify_content(
        parent,
        inspect_config=inspect_config,
        deidentify_config=deidentify_config,
        item=item,
    )

    # add a regex filter for email/phone for some extra insurance
    redacted = regex_filter(response.item.value)

    return redacted


def regex_filter(value):
    """Regex filter for phone and email address patterns. phone_regex is a little greedy so be careful passing
    through fields with ID numbers and so forth"""
    phone_regex = '(\d{3}[-\.]\d{3}[-\.]\d{4}|\(\d{3}\)*\d{3}[-\.]\d{4}|\d{3}[-\.]\d{4})'
    email_regex = '\S+@\S+'
    value = re.sub(phone_regex, '#########', value)
    value = re.sub(email_regex, '####', value)
    return value


def time_to_seconds(t):
    ts = ciso8601.parse_datetime(t)
    return int(time.mktime(ts.timetuple()))


def upload_file_gcs(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    # bucket_name = "your-bucket-name"
    # source_file_name = "local/path/to/file"
    # destination_blob_name = "storage-object-name"

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(
        "File {} uploaded to {}.".format(
            source_file_name, destination_blob_name
        )
    )
    os.remove(source_file_name)


def json_to_gcs(path, json_object, bucket_name):
    blob = storage.Blob(
        name=path,
        bucket=storage_client.get_bucket(bucket_name),
    )
    blob.upload_from_string(
        # dataflow needs newline-delimited json, so use ndjson
        data=ndjson.dumps(json_object),
        content_type='application/json',
        client=storage_client,
    )
    logging.info(
        'Successfully uploaded blob %r to bucket %r.', path, bucket_name)


# bash command to convert shapefiles to .geojson:
# for filename in ./*.shp; do mkdir -p geojson; ogr2ogr -f "GeoJSON" "./geojson/$filename.geojson" "$filename";done
# TODO: wrap this into a helper function
# TODO: helper to convert geojson -> json
