from __future__ import absolute_import

import os
import argparse
import io

import numpy as np
import pandas as pd
from google.cloud import storage

from dataflow_utils import pandas_utils
from dataflow_utils.pandas_utils import camel_to_snake_case, conv_avsc_to_bq_schema, fill_leading_zeroes, \
    strip_char_pattern, swap_field_names

storage_client = storage.Client()
bucket = storage_client.bucket(f"{os.environ['GCS_PREFIX']}_intime")

parser = argparse.ArgumentParser()
parser.add_argument('--input', dest='input', required=True,
                    help='fully specified GCS location from which to retrieve the source json file')
args = vars(parser.parse_args())

blob = bucket.blob(args['input'])
content = blob.download_as_string()
df = pd.read_json(io.BytesIO(content), lines=True)
df = pd.DataFrame.from_records(df)

assignment_fields = ['units', 'units', 'ranks', 'ranks']
demo_fields = ['attributes', 'attributes', 'workGroupData']
nested_asg_fields = ['name', 'validFrom', 'rankName', 'validFrom']
nested_demo_fields = ['attributeValue', 'attributeValue', 'workGroupName']
new_asg_field_names = ['unit', 'unit_valid_date', 'rank', 'rank_valid_date']
new_demo_field_names = ['race', 'gender', 'employee_type']
demo_search_fields = [('attributeName', 'Race'), ('attributeName', 'Gender'), '']
source_fields = assignment_fields.copy()
source_fields.extend(['contacts', 'attributes', 'workGroupData'])

df['email'] = None
for index, row in df.iterrows():
    email = None
    try:
        if df.loc[index, 'contacts']['infos']['info'].endswith('@pittsburghpa.gov'):
            email = df.loc[index, 'contacts']['infos']['info']
    except TypeError:
        contact_info = df.loc[index, 'contacts']
        if type(contact_info) is dict:
            for item in contact_info['infos']:
                if item['info'].endswith('@pittsburghpa.gov'):
                    email = item['info']
                    break
        elif type(contact_info) is list:
            for item in contact_info:
                if item['type'] == 'EMAIL':
                    email = item['infos']['info']
        else:
            email = None
    df.loc[index, 'email'] = email

for i in range(len(assignment_fields)):
    df[new_asg_field_names[i]] = None
    for index, row in df.iterrows():
        try:
            df.loc[index, new_asg_field_names[i]] = df.loc[index, assignment_fields[i]][nested_asg_fields[i]]
        except TypeError:
            try:
                for item in df.loc[index, assignment_fields[i]]:
                    try:
                        if 'validTo' not in item:
                            try:
                                df.loc[index, new_asg_field_names[i]] = item[nested_asg_fields[i]]
                            except KeyError:
                                pass
                            except TypeError:
                                for nested_item in item:
                                    if 'validTo' not in nested_item:
                                        try:
                                            df.loc[index, new_asg_field_names[i]] = nested_item[nested_asg_fields[i]]
                                        except KeyError:
                                            pass
                    except TypeError:
                        pass
            except TypeError:
                pass
        except KeyError:
            pass

for i in range(len(demo_fields)):
    if demo_search_fields[i]:
        df[new_demo_field_names[i]] = df[demo_fields[i]].apply(
            lambda x: next((item[nested_demo_fields[i]] for item in x if item[demo_search_fields[i][0]] == demo_search_fields[i][1]), None)
            if isinstance(x, list) else None
        )
    else:
        df[new_demo_field_names[i]] = df[demo_fields[i]].apply(
            lambda x: x[nested_demo_fields[i]] if isinstance(x, dict) and nested_demo_fields[i] in x else None
        )

df.drop(source_fields, axis=1, inplace=True)
df.rename(columns=lambda x: camel_to_snake_case(x), inplace=True)

field_name_swaps = [('middle_name', 'middle_initial'), ('external_id', 'mpoetc_number'),
                    ('other_id', 'badge_number'), ('anniversary_date', 'hire_date')]
df = swap_field_names(df, field_name_swaps)

df = strip_char_pattern(df, ['badge_number'], "(?<=\d)\.0$")
df['badge_number'] = df['badge_number'].replace('nan', None)
df = fill_leading_zeroes(df, 'employee_id', 6)

df['ncic_username'] = df['mpoetc_number']
df = fill_leading_zeroes(df, 'ncic_username', 6)
df['ncic_username'] = np.where(df['ncic_username'].notnull(), '~ALCPP' +
                               df['ncic_username'].astype(str), df['ncic_username'])

# convert all different Null types to a single type (None)
df = df.where(df.notnull(), None)

keep_fields = ['employee_id', 'mpoetc_number', 'ncic_username', 'badge_number', 'first_name', 'middle_initial',
               'last_name', 'display_name', 'email', 'birth_date', 'hire_date', 'rank', 'rank_valid_date', 'unit',
               'unit_valid_date', 'race', 'gender', 'employee_type']

df = df[keep_fields]
df = df.reindex(columns=keep_fields)

exclude = ['123456', 'Test1234', 'Test12345', 'Test123456', '009999']
df = df[~df['employee_id'].isin(exclude)]

#  read in AVRO schema and load into BQ
schema = conv_avsc_to_bq_schema(F"{os.environ['GCS_PREFIX']}_avro_schemas", "intime_employees.avsc")
df.to_gbq("intime.employee_data_pandas", project_id=f"{os.environ['GCLOUD_PROJECT']}",
          if_exists="replace", table_schema=schema)
