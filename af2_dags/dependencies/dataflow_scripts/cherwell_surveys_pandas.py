from __future__ import absolute_import

import logging
import os
import argparse
import json
import ndjson
import re
import io
import pytz
import dateutil

import pandas as pd
from google.cloud import storage

import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter


def avro_to_gcs(file_name, avro_object_list, bucket_name, schema_name):
    """
    take list of dicts in memory and upload to GCS as AVRO
    :params
    :file_name: str file name ending in ".avro" example-> "test.avro"
    :avro_object_list: a list of dictionaries. each dictionary is a single row of all fields
    :bucket_name: str of destination bucket. for avro files to be pushed into BQ and then deleted (the most common
    use case) this will be F"{os.environ['GCS_PREFIX']}_hot_metal"
    :schema_name: str containing the schema name example -> "test_schema.avsc"
    """
    avro_bucket = F"{os.environ['GCS_PREFIX']}_avro_schemas"
    blob = storage.Blob(
        name=schema_name,
        bucket=storage_client.get_bucket(avro_bucket),
    )

    schema_text = blob.download_as_string()
    schema = avro.schema.Parse(schema_text)
    writer = DataFileWriter(open(file_name, "wb"), DatumWriter(), schema)
    for item in avro_object_list:
       writer.append(item)
    writer.close()

    upload_file_gcs(bucket_name, file_name)


def change_data_type(df, convs):
    df = df.astype(convs)
    return df


def json_linter(ndjson: str):
    result_ndjson = []
    for i, line in enumerate(ndjson.split('\n')):
        try:
            json.loads(line)
            result_ndjson.append(line)
        except:
            json_split = line.split('}{')
            for idx in range(len(json_split)):
                if idx == 0:
                    result_ndjson.append(json_split[idx] + '}')
                elif idx == (len(json_split)-1):
                    result_ndjson.append('{' + json_split[idx])
                else:
                    result_ndjson.append('{' + json_split[idx] + '}')

    return '\n'.join(result_ndjson)


def json_to_gcs(path, json_object_list, bucket_name):
    """
    take list of dicts in memory and upload to GCS as newline JSON
    """
    blob = storage.Blob(
        name=path,
        bucket=storage_client.get_bucket(bucket_name),
    )
    try:
        blob.upload_from_string(
            # dataflow needs newline-delimited json, so use ndjson
            data=ndjson.dumps(json_object_list),
            content_type='application/json',
            client=storage_client,
        )
    except json.decoder.JSONDecodeError:
        print("Error uploading data to GCS bucket, linting and trying again")
        str_requests = ndjson.dumps(json_object_list)
        linted = "[" + json_linter(str_requests) + "]"
        linted_requests = ndjson.loads(linted)[0]
        json_to_gcs(path, linted_requests, bucket_name)

    logging.info('Successfully uploaded blob %r to bucket %r.', path, bucket_name)

    print('Successfully uploaded blob {} to bucket {}'.format(path, bucket_name))


def standardize_times(df, time_changes):
    result = {}

    for time_change in time_changes:
        if df[time_change[0]] is not None and df[time_change[0]] != '':
            parse_dt = dateutil.parser.parse(df[time_change[0]])
            clean_dt = parse_dt.replace(tzinfo=None)
            try:
                pytz.all_timezones.index(time_change[1])
            except ValueError:
                pass
            else:
                loc_time = pytz.timezone(time_change[1]).localize(clean_dt, is_dst=None)
                utc_conv = loc_time.astimezone(tz=pytz.utc)
                east_conv = loc_time.astimezone(tz=pytz.timezone('US/Eastern'))
                unix_conv = utc_conv.timestamp()
                result['{}_UTC'.format(time_change[0])] = utc_conv.strftime('%Y-%m-%d %H:%M:%S')
                result['{}_EST'.format(time_change[0])] = east_conv.strftime('%Y-%m-%d %H:%M:%S')
                result['{}_UNIX'.format(time_change[0])] = int(unix_conv)
        else:
            result['{}_UTC'.format(time_change[0])] = None
            result['{}_EST'.format(time_change[0])] = None
            result['{}_UNIX'.format(time_change[0])] = None

    return pd.Series(result)


def upload_file_gcs(bucket_name, file):
    """
    Uploads a file to the bucket.
    param bucket_name:str = "your-bucket-name" where the file will be placed
    param source_file:str = "local/path/to/file"
    param destination_blob_name:str = "storage-object-name"
    """

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file)
    blob.upload_from_filename(file)

    os.remove(file)


storage_client = storage.Client()
bucket = storage_client.bucket(f"{os.environ['GCS_PREFIX']}_cherwell")
hot_bucket = f"{os.environ['GCS_PREFIX']}_hot_metal"

parser = argparse.ArgumentParser()
parser.add_argument('--input', dest='input', required=True,
                    help='fully specified GCS location from which to retrieve the source json file')
# parser.add_argument('--json_output_arg', dest = 'json_out_loc', required = True,
#                     help = 'fully specified location to upload the processed json file for long-term storage')
args = vars(parser.parse_args())

blob = bucket.blob(args['input'])
content = blob.download_as_string()
df = pd.read_json(io.BytesIO(content), lines=True)
df = pd.DataFrame.from_records(df['fields'])

for col in df.columns:
    # Extract new column name and values
    new_column_name = df[col].apply(lambda x: x['name'])
    new_column_value = df[col].apply(lambda x: x['value'])

    # Rename the column and assign new values
    df.rename(columns={col: new_column_name[0]}, inplace=True)
    df[new_column_name[0]] = new_column_value

new_field_names = ['id', 'incident_id', 'created_date', 'submitted_by', 'submitted_date', 'survey_complete',
                   'q1_timely_resolution', 'q2_handled_professionally', 'q3_clear_communication',
                   'q4_overall_satisfaction', 'q5_request_handled_first_time', 'q6_improvement_suggestions',
                   'q7_contact_me', 'q8_additional_comments', 'survey_score', 'avg_survey_score',
                   'owned_by', 'last_modified_date', 'last_modified_by']
df.columns = new_field_names

time_cols = ['created_date', 'submitted_date', 'last_modified_date']
times = [('created_date', 'US/Eastern'), ('submitted_date', 'US/Eastern'), ('last_modified_date', 'US/Eastern')]
time_df = df.apply(lambda x: standardize_times(x, times), axis=1)
df = df.drop(columns=time_cols).join(time_df)

df['submitted_date_UNIX'] = df['submitted_date_UNIX'].fillna(0)
convs = {'id': str, 'incident_id': str, 'q1_timely_resolution': int,
         'q2_handled_professionally': int, 'q3_clear_communication': int,
         'q4_overall_satisfaction': int,  'submitted_date_UNIX': int,
         'survey_score': float, 'avg_survey_score': float}
df = change_data_type(df, convs)
df['submitted_date_UNIX'] = df['submitted_date_UNIX'].mask(df['submitted_date_UNIX'] == 0, None)
df['survey_complete'] = df['survey_complete'].map({'True': True, 'False': False})

# convert all different Null types to a single type (None)
df = df.where(df.notnull(), None)

out_data = df.to_dict(orient='records')
# json_to_gcs(args['json_out_loc'], out_data, bucket)
avro_to_gcs('survey_responses.avro', out_data, hot_bucket, "cherwell_surveys.avsc")
