from __future__ import print_function

import os
import logging
import time
import re

import ciso8601
import ckanapi
import ndjson

from google.cloud import storage, dlp_v2

storage_client = storage.Client()
dlp = dlp_v2.DlpServiceClient()
project = os.environ['GCLOUD_PROJECT']

WPRDC_API_HARD_LIMIT = 500001  # A limit set by the CKAN instance.


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
    """
    convert YYYY-MM-DD to seconds
    :param t: date as YYYY-MM-DD (string)
    :return: int (time in seconds)
    """
    ts = ciso8601.parse_datetime(t)
    return int(time.mktime(ts.timetuple()))


def swap_field_names(datum, name_changes):
    """
    change/clean field names in result dict

    :param datum: dict
    :param name_changes: tuple consisting of existing field name + name to which it should be changed
    :return: dict with updated field names
    """
    for name_change in name_changes:
        datum[name_change[1]] = datum[name_change[0]]
        del datum[name_change[0]]

    return datum


def change_data_types(datum, type_changes):
    """
    change data types

    :param datum: dict
    :param type_changes: list of tuples of the fields to change data type
    :return: dict with updated data types
    """
    try:
        for type_change in type_changes:
            if type_change[1] is "float":
                datum[type_change[0]] = float(datum[type_change[0]])
            elif type_change[1] is "int":
                datum[type_change[0]] = int(datum[type_change[0]])
            elif type_change[1] is "str":
                datum[type_change[0]] = str(datum[type_change[0]])
            elif type_change[1] is "bool":
                datum[type_change[0]] = bool(datum[type_change[0]])
    except TypeError:
        pass

    return datum


def filter_fields(results, relevant_fields, name_changes = None):
    """
    Remove unnecessary keys from results, optionally rename fields

    :param results: list of dicts
    :param relevant_fields: list of field names to preserve
    :param name_changes: list of tuples comprising original field name + new name (optional)
    :return: transformed list of dicts
    """
    trimmed_results = []
    for result in results:
        trimmed_result = {k: result[k] for k in relevant_fields}
        if name_changes is not None:
            trimmed_result = swap_field_names(trimmed_result, name_changes)
        trimmed_results.append(trimmed_result)

    return trimmed_results


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


def json_to_gcs(path, json_object, bucket_name):
    """
    take list of dicts in memory and upload to GCS as newline JSON
    """
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

    print('Successfully uploaded blob {} to bucket {}'.format(path, bucket_name))


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


def synthesize_query(resource_id, select_fields=['*'], where_clauses=None, group_by=None, order_by=None, limit=None):
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

The field names should usually be surrounded by double quotes (unless they are snake case field names), and string values need to be surrounded by single quotes.
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
SELECT COUNT("DogName") AS amount, "DogName" FROM "f8ab32f7-44c7-43ca-98bf-c1b444724598" WHERE "Breed" = 'POODLE STANDARD' GROUP BY "DogName" ORDER BY amount DESC LIMIT 5
Here are the resulting top five names for the POODLE STANDARD breed, sorted by decreasing frequency:
[{'DogName': 'HERSHEY', 'amount': '4'},
 {'DogName': 'COCO ', 'amount': '3'},
 {'DogName': 'BUDDY', 'amount': '3'},
 {'DogName': 'MOLLY', 'amount': '3'},
 {'DogName': 'MASON', 'amount': '3'}]
"""


def get_wprdc_data(resource_id, select_fields=['*'], where_clauses=None, group_by=None, order_by=None, limit=None,
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
            f"Note that there may be more results than you have obtained since the WPRDC CKAN instance only returns "
            f"{WPRDC_API_HARD_LIMIT} records at a time.")
        # If you send a bogus SQL query through to the CKAN API, the resulting error message will include the full
        # query used by CKAN, which wraps the query you send something like this: "SELECT * FROM (<your query>) LIMIT
        # 500001", so you can determine the actual hard limit that way.

    # Clean out fields that no one needs.
    records = remove_fields(records, ['_full_text', '_id'])
    records = remove_fields(records, fields_to_remove)
    return records

# TODO: function to convert CSV or SQL result to pandas df -> json_to_gcs

# TODO: helper to convert geojson -> ndjson

# bash command to convert shapefiles to .geojson:
# for filename in ./*.shp; do mkdir -p geojson; ogr2ogr -f "GeoJSON" "./geojson/$filename.geojson" "$filename";done
# TODO: wrap this into a helper function
