from __future__ import print_function

import argparse
import os
import logging
import time
import re
import json
import ckanapi
import ndjson
import pytz
import requests
import pandas as pd
import jaydebeapi

import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

from datetime import datetime
from google.cloud import storage, dlp

storage_client = storage.Client()
dlp_client = dlp.DlpServiceClient()

PROJECT = os.environ['GCLOUD_PROJECT']
USER_DEFINED_CONST_BUCKET = "user_defined_data"
DEFAULT_PII_TYPES = [{"name": "PERSON_NAME"}, {"name": "EMAIL_ADDRESS"}, {"name": "PHONE_NUMBER"}]

COMPUTRONIX_BASE_URL = 'https://staff.onestoppgh.pittsburghpa.gov/pghprod/odata/odata/'
WPRDC_API_HARD_LIMIT = 500001  # A limit set by the CKAN instance.


def snake_case_place_names(input):
    # Helper function to take a pair of words, containing place name identifiers, and join them together (with an
    # underscore by default). This prevents NLP based Data Loss Prevention/PII scrubbers from targeting places for
    # name based redaction (e.g. avoiding redacting "Schenley" from "Schenley Park"), because the GCP tools will not
    # treat the joined phrase as person's name. This approach should be phased out after a less brittle and more elegant
    # tool is developed.

    bucket = storage_client.get_bucket(USER_DEFINED_CONST_BUCKET)
    blob = bucket.blob('place_identifiers.txt')
    place_name_identifiers = blob.download_as_string().decode('utf-8')

    # if an identifier is found (indicative of a place such as a road or park), we want to join the place with the
    # preceding word with the join character. Thus, "Moore Park" would become "Moore_Park".
    joined_places = (re.sub(r'(\s)\b({})\b'.format(place_name_identifiers), r'_\2', input,
                            flags=re.IGNORECASE))

    return joined_places


def replace_pii(input_str, retain_location: bool, info_types=DEFAULT_PII_TYPES):
    # This helper function added 2021-07-26 to update the existing methodology (deprecated below).
    # configure API client call arguments (incuding a full resource id for the project)

    if retain_location:
        input_str = snake_case_place_names(input_str)

    item = {"value": input_str}
    max_findings = 0
    include_quote = False
    inspect_config = {
        "info_types": info_types,
        "include_quote": include_quote,
        "limits": {"max_findings_per_request": max_findings},
    }
    deidentify_config = {
        "info_type_transformations": {
            "transformations": [
                {"primitive_transformation": {"replace_with_info_type_config": {}}}
            ]
        }
    }
    parent = "projects/{}".format(PROJECT)

    response = dlp_client.deidentify_content(parent, deidentify_config, inspect_config, item)

    return response.item.value


# def scrub_pii(field, data_objects):
#     """You could reasonably make a case for doing this in the Dataflow portion of the DAG, but IMHO it's better to
#     catch PII before it even gets to Cloud Storage; if we filter it at the Dataflow stage it won't make it to
#     BigQuery,
#     but will still be in GCS -- james 2/6/20"""
#
#     for data_object in data_objects:
#         # make sure comments field isn't empty; otherwise DLP API throws an error
#         if data_object[field].strip(' '):
#             data_object[field] = get_dlp_redaction(data_object[field])
#         # google's DLP API has a rate limit of 600 requests/minute
#         # TODO: consider a different workaround here; not robust for large datasets
#         if data_objects.index(data_object) % 600 == 0 and data_objects.index(data_object) != 0:
#             time.sleep(61)
#
#     return data_objects
#
#
# def get_dlp_redaction(uncleaned_string):
#     # remove newline delimiter
#     uncleaned_string = uncleaned_string.replace('\n', ' ')
#     parent = dlp.project_path(project)
#
#     # Construct inspect configuration dictionary
#     info_types = ["EMAIL_ADDRESS", "FIRST_NAME", "LAST_NAME", "PHONE_NUMBER", "URL", "STREET_ADDRESS"]
#     inspect_config = {"info_types": [{"name": info_type} for info_type in info_types]}
#
#     # Construct deidentify configuration dictionary
#     deidentify_config = {
#         "info_type_transformations": {
#             "transformations": [
#                 {
#                     "primitive_transformation": {
#                         "character_mask_config": {
#                             "masking_character": "#",
#                             "number_to_mask": 0,
#                         }
#                     }
#                 }
#             ]
#         }
#     }
#
#     # Construct item
#     item = {"value": uncleaned_string}
#
#     # Call the API
#     response = dlp.deidentify_content(
#         parent,
#         inspect_config=inspect_config,
#         deidentify_config=deidentify_config,
#         item=item,
#     )
#
#     # add a regex filter for email/phone for some extra insurance
#     redacted = regex_filter(response.item.value)
#
#     return redacted


def regex_filter(value):
    """Regex filter for phone and email address patterns. phone_regex is a little greedy so be careful passing
    through fields with ID numbers and so forth"""
    phone_regex = '(\d{3}[-\.]\d{3}[-\.]\d{4}|\(\d{3}\)*\d{3}[-\.]\d{4}|\d{3}[-\.]\d{4})|\d{10}'
    email_regex = '\S+@\S+'
    value = re.sub(phone_regex, '#########', value)
    value = re.sub(email_regex, '####', value)
    return value


def time_to_seconds(t):
    """
    convert YYYY-MM-DD (UTC) to seconds
    :param t: date as YYYY-MM-DD (string)
    :return: int (time in seconds, 12:00 AM UTC)
    """
    ts = datetime.strptime(t, '%Y-%m-%d')
    return int(ts.replace(tzinfo=pytz.UTC).timestamp())


def filter_fields(results, relevant_fields, add_fields=True):
    """
    Remove unnecessary keys from results or filter for only those you want depending on add_fields arg
    :param results: list of dicts
    :param relevant_fields: list of field names to preserve
    :param add_fields: (boolean/optional) preserve or remove the values passed in the relevant_fields parameter.
    In the case that there are many fields we want to keep and just a few we want to remove, it's more useful to pass
    add_fields=False and then pass the fields we want to remove in the relevant_fields param. Defaults to True

    :return: transformed list of dicts
    """
    trimmed_results = []
    for result in results:
        if add_fields:
            trimmed_result = {k: result[k] for k in relevant_fields}
        else:
            trimmed_result = {k: result[k] for k in result if k not in relevant_fields}
        trimmed_results.append(trimmed_result)

    return trimmed_results


def roll_up_coords(datum, coord_fields):
    """
    Takes a datum with lat + long fields and trims those fields to 3 decimal places (200-meter radius) for privacy
    :param datum: dict
    :param coord_fields: tuple (lat field name + long field name)
    :return: dict
    """
    datum[coord_fields[0]] = round(float(datum[coord_fields[0]]), 3)
    datum[coord_fields[1]] = round(float(datum[coord_fields[1]]), 3)
    return datum


def execution_date_to_quarter(execution_date):
    """
    :param execution_date: DAG execution date, passed through via Airflow template variable
    :return: quarter, year as int (e.g. 'Q1', 2020)
    """

    split_date = execution_date.split('-')
    year = split_date[0]
    day = split_date[1] + '-' + split_date[2]
    if '01-01' <= day < '04-01':
        quarter = 'Q1'
    elif '04-01' <= day < '07-01':
        quarter = 'Q2'
    elif '07-01' <= day < '10-01':
        quarter = 'Q3'
    else:
        quarter = 'Q4'

    return quarter, int(year)


def execution_date_to_prev_quarter(execution_date):
    """
    :param execution_date: DAG execution date, passed through via Airflow template variable
    :return: quarter, year as int (e.g. 'Q1', 2020)
    """

    split_date = execution_date.split('-')
    year = split_date[0]
    day = split_date[1] + '-' + split_date[2]
    if '01-01' <= day < '04-01':
        quarter = 'Q4'
        year = int(year) - 1
    elif '04-01' <= day < '07-01':
        quarter = 'Q1'
    elif '07-01' <= day < '10-01':
        quarter = 'Q2'
    else:
        quarter = 'Q3'

    return quarter, int(year)


def sql_to_dict_list(conn, sql_query, db='mssql', date_col=None, date_format=None):
    """
    Execute sql query and return list of dicts
    :param conn: sql db connection
    :param sql_query: str
    :param db: database type (cursor result syntax differs)
    :param date_col: str - dataframe column to be converted from datetime object to string
    :param date_format: str (format for conversion of datetime object to date string)
    :return: query results as list of dicts
    """
    cursor = conn.cursor()
    cursor.execute(sql_query)
    field_names = [i[0] for i in cursor.description]

    if db == 'mssql':
        results = [result for result in cursor]
    elif db == 'oracle':
        results = cursor.fetchall()

    df = pd.DataFrame(results)
    df.columns = field_names

    if date_col is not None:
        df[date_col] = df[date_col].apply(lambda x: x.strftime('%Y-%m-%d'))

    results_dict = df.to_dict('records')

    return results_dict


def upload_file_gcs(bucket_name, source_file_name, destination_blob_name):
    """
    Uploads a file to the bucket.
    param bucket_name:str = "your-bucket-name"
    param source_file_name:str = "local/path/to/file"
    param destination_blob_name:str = "storage-object-name"
    """

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print("File {} uploaded to {}.".format(source_file_name, destination_blob_name))

    os.remove(source_file_name)


def avro_to_gcs(path, file_name, avro_object_list, bucket_name, schema_def):
    """
    take list of dicts in memory and upload to GCS as AVRO
    """
    avro_bucket = "pghpa_avro_schemas"
    blob = storage.Blob(
        name=schema_def,
        bucket=storage_client.get_bucket(avro_bucket),
    )

    schema_text = blob.download_as_string()
    schema = avro.schema.Parse(schema_text)

    writer = DataFileWriter(open(file_name, "wb"), DatumWriter(), schema)
    for item in avro_object_list:
       writer.append(item)
    writer.close()

    upload_file_gcs(bucket_name, file_name, f"{path}/{file_name}")

    logging.info(
        'Successfully uploaded blob %r to bucket %r.', path, bucket_name)

    print('Successfully uploaded blob {} to bucket {}'.format(path, bucket_name))


def json_to_gcs(path, json_object_list, bucket_name):
    """
    take list of dicts in memory and upload to GCS as newline JSON
    """
    blob = storage.Blob(
        name=path,
        bucket=storage_client.get_bucket(bucket_name),
    )
    blob.upload_from_string(
        # dataflow needs newline-delimited json, so use ndjson
        data=ndjson.dumps(json_object_list),
        content_type='application/json',
        client=storage_client,
    )
    logging.info(
        'Successfully uploaded blob %r to bucket %r.', path, bucket_name)

    print('Successfully uploaded blob {} to bucket {}'.format(path, bucket_name))


def get_computronix_odata(endpoint, params=None, expand_fields=None):
    """
    Hit the Computronix odata feed and loop through all result pages, storing results in a list of dicts
    :param endpoint (str): API endpoint, e.g. 'DOMIPERMIT'
    :param params (list): params for odata query, e.g. ['$orderby=CREATEDDATE%20desc', '$top=1000']
    :param expand_fields (list): fields in odata results to expand, e.g. ['ADDRESS']
    :return: list of dicts
    """
    records = []
    more_links = True
    odata_url = COMPUTRONIX_BASE_URL + endpoint

    if params or expand_fields:
        odata_url += '?'
    if params:
        for param in params:
            odata_url += F'{param}&'
    if expand_fields:
        odata_url += F'$expand='
        for field in expand_fields:
            odata_url += F'{field},'
    while more_links:
        try:
            res = requests.get(odata_url)
            records.extend(res.json()['value'])
            if '@odata.nextLink' in res.json().keys():
                odata_url = res.json()['@odata.nextLink']
            else:
                more_links = False
        except requests.exceptions.RequestException:
            more_links = False

    return records


def select_expand_odata(url, tables, limit_results=False):
    """
        General ODATA API query generator. This function will format the query, request from the API, and loop through
        all result pages, Data are returned as a list of dicts. Note- This function is used for Selects and Joins (
        expansions) ONLY. More complicated queries need to be customized.

        :param url (str): full URL base for the API (not including the base table that will form the beginning of the query
        :param tables (list): List of tuples. Each tuple contains the following elements in this order-
            [
                ( 'Table Name', ['Table(s) to form nested expansion on'], [String(s) of columns to select from the
                tables] )
            ]
            The first value in each tuple is a string. The second is a LIST of strings (still put the string in a
            list even if it is only value). The third is a list of strings.
            EXAMPLE:
            tables = [
                        ("base_table",False,["id"]),
                        ("abc",False,["name"]),
                        ("def",["hij","klm"],["phone", "address", "DOB, age"])
                    ]

            The input above would 1) select the property "id" from "base_table". 2) expand the above data on "abc"
            and select "name" from "abc". 3) expand the above data on table "def" and select "phone" from that table.
            4) perform a nested expansion of both "hij" and "klm" on "def". "address" will be selected from "hij" and
            "DOB, age" will be selected from "klm". Remember- this will nest "hij" and "klm" into "def" expansion.
            This will NOT nest "klm" in "hij"

            All operations will begin with the base_table

            To simply select all columns from a table use a star in the third value of the tuple (e.g. ["*"])

            Insert a False into the second value of the table to avoid nested expansions. If only 1 table is needed,
            just use 1 tuple.
        :return: list of dicts representing ODATA API results
        """

    odata_url = F"{url}{tables[0][0]}?$select={tables[0][2][0]}"

    # if more than one table
    if len(tables) > 1:
        odata_url += ',&$expand='

        for t in tables[1:]:
            odata_url += F'{t[0]}($select={t[2][0]}'
            if t[1]:
                for (sub_t, sub_cols) in zip(t[1], t[2][1:]):
                    odata_url += F',;$expand={sub_t}($select={sub_cols})'

            odata_url += '),'

    more_links = True
    records = []
    while more_links:
        try:
            res = requests.get(odata_url)
            records.extend(res.json()['value'])
            if limit_results:
                more_links = False
            elif '@odata.nextLink' in res.json().keys():
                odata_url = res.json()['@odata.nextLink']
            else:
                more_links = False
        except requests.exceptions.RequestException:
            more_links = False

    return records


def query_resource(site, query):
    """Use the datastore_search_sql API endpoint to query a public CKAN resource."""
    ckan = ckanapi.RemoteCKAN(site)
    response = ckan.action.datastore_search_sql(sql=query)
    data = response['records']
    # Note that if a CKAN table field name is a Postgres reserved word (like
    # ALL or CAST or NEW), you get a not-very-useful error
    #      (e.g., 'query': ['(ProgrammingError) syntax error at or near
    #     "on"\nLINE 1: SELECT * FROM (SELECT load, on FROM)
    # and you need to escape the reserved field name with double quotes.
    # It's actually best to escape all field names with double quotes,
    # but if it's all lowercase letters and underscores in the CKAN table,
    # you can get away with not escaping it in your query.

    return data


def query_any_resource(resource_id, query):
    """This function is a wrapper around query_resource. This wrapper just checks
    whether a resource is private and returns an explanation of why it can't be
    queried if it is private. Otherwise it returns the query_resource results."""
    site = "https://data.wprdc.org"
    ckan = ckanapi.RemoteCKAN(site)
    # From resource ID, determine package ID.
    package_id = ckan.action.resource_show(id=resource_id)['package_id']
    # From package ID, determine if the package is private.
    private = ckan.action.package_show(id=package_id)['private']
    if private:
        print(
            "As of February 2018, CKAN still doesn't allow you to run a datastore_search_sql query on a private "
            "dataset. Sorry. See this GitHub issue if you want to know a little more: "
            "https://github.com/ckan/ckan/issues/1954")
        raise ValueError("CKAN can't query private resources (like {}) yet.".format(resource_id))
    else:
        return query_resource(site, query)


def intersection(list1, list2):
    return list(set(list1) & set(list2))


def validate_where_clauses(where_clause):
    """This function provides a little validation of a single where clause
    by ensuring that it contains an operator."""
    operators = ['=', '>', '<', '>=', '<=', '<>', '!=', 'BETWEEN', 'LIKE', 'IN']
    parts = [p.upper() for p in where_clause.split(' ')]
    if not intersection(operators, parts):
        raise ValueError(f"No operator found in the WHERE clause {where_clause}.")


def remove_fields(records, fields_to_remove):
    """This function removes selected fields from the CKAN records. The intent is
    to remove the '_full_text' field, which is row-level metadata to facilitate
    searches of the records, but this function could be used to purge other
    fields, like '_geom' and '_the_geom_webmercator', which may not be of
    interest in some situations."""
    for r in records:
        _ = [r.pop(key, None) for key in fields_to_remove]
    return records


def synthesize_query(resource_id, select_fields=['*'], where_clauses=None, group_by=None, order_by=None,
                     limit=None):
    query = f'SELECT {", ".join(select_fields)} FROM "{resource_id}"'

    if where_clauses is not None:
        # for clause in list(where_clauses):
        #     validate_where_clause(clause)
        # query += f" WHERE {' AND '.join(where_clauses)}"

        validate_where_clauses(where_clauses)
        query += 'WHERE ' + where_clauses
    if group_by is not None:
        query += f" GROUP BY {group_by}"
    if order_by is not None:
        query += f" ORDER BY {order_by}"
    if limit is not None:
        try:
            int(limit)
        except ValueError:
            print(f"Unable to cast the LIMIT parameter '{limit}' to an integer limit.")
        query += f" LIMIT {limit}"

    return query


"""
The query parameters sent to the get_wprdc_data function look like this:
{'resource_id': 'f8ab32f7-44c7-43ca-98bf-c1b444724598',
 'select_fields': ['*'],
 'where_clauses': ['"DogName" LIKE \'DOGZ%\'']}
The resulting query is:
SELECT * FROM "f8ab32f7-44c7-43ca-98bf-c1b444724598" WHERE "DogName" LIKE 'DOGZ%'
The field names should usually be surrounded by double quotes (unless they are snake case field names), and string 
values need to be surrounded by single quotes.
Executing the query fetches 1 record.
The first record looks like this:
{'Breed': 'BOSTON TERRIER',
 'Color': 'BRINDLE',
 'DogName': 'DOGZILLA',
 'ExpYear': 2099,
 'LicenseType': 'Dog Lifetime Spayed Female',
 'OwnerZip': '15102',
 'ValidDate': '2013-03-28T11:38:56',
 '_geom': None,
 '_id': 27210,
 '_the_geom_webmercator': None}
Here's another query, just getting dog names that contain 'CAT':
SELECT "DogName" AS name FROM "f8ab32f7-44c7-43ca-98bf-c1b444724598" WHERE "DogName" LIKE '%CAT%'
The returned list of records looks like this:
[{'name': 'CAT STEVENS'},
 {'name': 'CATO'},
 {'name': 'CATCHER'},
 {'name': 'CATALINA'},
 {'name': 'CATEY'},
 {'name': 'GRAYSON MERCATORIS'}]
Finally, let's test some other query elements. Here's the query:
SELECT COUNT("DogName") AS amount, "DogName" FROM "f8ab32f7-44c7-43ca-98bf-c1b444724598" WHERE "Breed" = 'POODLE 
STANDARD' GROUP BY "DogName" ORDER BY amount DESC LIMIT 5
Here are the resulting top five names for the POODLE STANDARD breed, sorted by decreasing frequency:
[{'DogName': 'HERSHEY', 'amount': '4'},
 {'DogName': 'COCO ', 'amount': '3'},
 {'DogName': 'BUDDY', 'amount': '3'},
 {'DogName': 'MOLLY', 'amount': '3'},
 {'DogName': 'MASON', 'amount': '3'}]
"""


def get_wprdc_data(resource_id, select_fields=['*'], where_clauses=None, group_by=None, order_by=None,
                   limit=None,
                   fields_to_remove=None):
    """
    helper to construct query for CKAN API and return results as list of dictionaries
    :param resource_id: str
    :param select_fields: list
    :param where_clauses: str
    :param group_by: str
    :param order_by: str
    :param limit: int
    :param fields_to_remove: list
    :return: results as list of dictionaries
    """
    query = synthesize_query(resource_id, select_fields, where_clauses, group_by, order_by, limit)
    records = query_any_resource(resource_id, query)

    if len(records) == WPRDC_API_HARD_LIMIT:
        print(
            f"Note that there may be more results than you have obtained since the WPRDC CKAN instance only "
            f"returns "
            f"{WPRDC_API_HARD_LIMIT} records at a time.")
        # If you send a bogus SQL query through to the CKAN API, the resulting error message will include the full
        # query used by CKAN, which wraps the query you send something like this: "SELECT * FROM (<your query>) LIMIT
        # 500001", so you can determine the actual hard limit that way.

    # Clean out fields that no one needs.
    records = remove_fields(records, ['_full_text', '_id'])
    if fields_to_remove is not None:
        records = remove_fields(records, fields_to_remove)

    return records


def rmsprod_setup():
    parser = argparse.ArgumentParser()
    parser.add_argument('-e', '--execution_date', dest='execution_date',
                        required=True, help='DAG execution date (YYYY-MM-DD)')
    parser.add_argument('-s', '--prev_execution_date', dest='prev_execution_date',
                        required=True, help='Previous DAG execution date (YYYY-MM-DD)')
    args = vars(parser.parse_args())
    execution_year = args['execution_date'].split('-')[0]
    execution_month = args['execution_date'].split('-')[1]
    execution_date = args['execution_date']
    bucket = '{}_police'.format(os.environ['GCS_PREFIX'])

    conn = jaydebeapi.connect("oracle.jdbc.OracleDriver",
                              os.environ['RMSPROD_DB'],
                              [os.environ['RMSPROD_UN'], os.environ['RMSPROD_PW']],
                              os.environ['DAGS_PATH'] + "/dependencies/gcs_loaders/ojdbc6.jar")

    return args, execution_year, execution_month, execution_date, bucket, conn


# TODO: helper to convert geojson -> ndjson

# bash command to convert shapefiles to .geojson:
# for filename in ./*.shp; do mkdir -p geojson; ogr2ogr -f "GeoJSON" "./geojson/$filename.geojson" "$filename";done
# TODO: wrap this into a helper function


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


def fix_nd_json_new_line_sep(nd_json: str):
    """
    :Author - Pranav Banthia
    :param nd_json - NDJson is a json file where each line is an individual json object. The delimiter is a new line \n
                    This function takes in a param called ndjson which is a string object.
    We parse each line of the string assuming that every line is an individual json object. If there are any exceptions
    such as two json objects on the same line then we handle that situation in the except block. Returns an ndjson
    as a string
    """
    result_ndjson = []
    for i, line in enumerate(nd_json.split('\n')):
        try:
            json.loads(line)
            result_ndjson.append(line)
        except json.JSONDecodeError:
            json_split = line.split('}{')
            result_ndjson.append(json_split[0] + '}')
            result_ndjson.append('{' + json_split[1])

    return '\n'.join(result_ndjson)
