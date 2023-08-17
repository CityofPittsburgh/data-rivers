import io
import json
import ndjson
import logging
import os
import pytz
import dateutil
import re

import pandas as pd
from google.cloud import storage
from google.cloud import bigquery

import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter

storage_client = storage.Client()
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


def camel_to_snake_case(val, strip_field=''):
    if strip_field:
        val = val.replace(strip_field, '')
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', val)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def conv_avsc_to_bq_schema(avro_bucket, schema_name):
    # bigquery schemas that are used to upload directly from pandas are not formatted identically as an avsc filie.
    # this func makes the necessary conversions. this allows a single schema to serve both purposes

    blob = storage.Blob(name=schema_name, bucket=storage_client.get_bucket(avro_bucket))
    schema_text = blob.download_as_string()
    schema = json.loads(schema_text)

    schema = schema['fields']

    new_schema = []
    change_vals = {"float": "float64", "integer": "int64"}
    change_keys = change_vals.keys()
    for s in schema:
        if 'null' in s["type"]: s["type"].remove('null')
        s["type"] = s["type"][0]
        if s["type"] in change_keys: s["type"] = change_vals[s["type"]]
        new_schema.append(s)

    return new_schema


def change_data_type(df, convs):
    df = df.astype(convs)
    return df


def df_to_partitioned_bq_table(df, dataset, table, avro_schema, partition_type="DAY", partition_field=None,
                               disposition="WRITE_TRUNCATE"):
    # adapted from https://stackoverflow.com/a/69666464
    client = bigquery.Client(project=f"{os.environ['GCLOUD_PROJECT']}")

    schema_list = []
    for field in avro_schema:
        try:
            schema_list.append(bigquery.SchemaField(name=field['name'], field_type=field['type'], mode=field['mode']))
        except:
            schema_list.append(bigquery.SchemaField(name=field['name'], field_type=field['type']))

    # configure BQ upload job with user-defined parameters (or default to WRITE_TRUNCATE/DAY partitioning if left blank)
    job_config = bigquery.LoadJobConfig(
        schema=schema_list,
        write_disposition=disposition,
        time_partitioning=bigquery.table.TimePartitioning(type_=partition_type, field=partition_field)
    )

    # execute BQ API request to load table
    job = client.load_table_from_dataframe(df, f"{os.environ['GCLOUD_PROJECT']}.{dataset}.{table}",
                                           job_config=job_config)
    # NOTE: pyarrow and numpy libraries must be up to date for this to work
    job.result()


def fill_leading_zeroes(df, field_name, digits):
    df[field_name] = df[field_name].astype(str)
    df[field_name] = df[field_name].apply(lambda x: x.zfill(digits) if not any(c.isalpha() for c in x) else None)
    return df


def find_last_successful_run(bucket_name, good_run_path, look_back_date):
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.get_blob(good_run_path)
    # if blobs are found
    if blob is not None:
        run_info = blob.download_as_string()
        last_good_run = ndjson.loads(run_info.decode('utf-8'))[0]["current_run"]
        first_run = False
        return last_good_run, first_run
    else:
        first_run = True
        return str(look_back_date), first_run


def gcs_to_df(bucket_name, file_name):
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    content = blob.download_as_string()
    df = pd.read_csv(io.BytesIO(content), encoding='utf-8')
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
                elif idx == (len(json_split) - 1):
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


def set_col_b_based_on_col_a_val(row, col_a, col_b, check_val, new_val):
    # adapted from https://stackoverflow.com/a/59796763
    a = row[col_a]
    b = row[col_b]
    if a == check_val:
        return new_val
    else:
        return b


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


def strip_char_pattern(df, fields, rgx):
    for field in fields:
        df[field] = df[field].astype(str)
        df[field] = df[field].apply(lambda x: re.sub(rgx, '', x) if isinstance(x, str) else x)

    return df


def swap_field_names(df, name_changes):
    for name_change in name_changes:
        try:
            df = df.rename(columns={name_change[0]: name_change[1]})
        except TypeError:
            print(f"{name_change[0]} and {name_change[1]} were not both found within dataframe")
            df[name_change[1]] = None
        except KeyError:
            print(f"{name_change[0]} not found as a field within dataframe")
            df[name_change[1]] = None

    return df


def swap_two_columns(df, col1, col2):
    col_list = list(df.columns)
    x, y = col_list.index(col1), col_list.index(col2)
    col_list[y], col_list[x] = col_list[x], col_list[y]
    df = df[col_list]
    return df


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
