from __future__ import absolute_import

import os
import argparse
import io

import pandas as pd
from google.cloud import storage

from dataflow_utils.pandas_utils import change_data_type, standardize_times

FINAL_COLS = ['id', 'incident_id', 'created_date_EST', 'created_date_UTC', 'created_date_UNIX', 'submitted_by',
              'submitted_date_EST', 'submitted_date_UTC', 'submitted_date_UNIX', 'survey_complete',
              'q1_timely_resolution', 'q2_handled_professionally', 'q3_clear_communication', 'q4_overall_satisfaction',
              'q5_request_handled_first_time', 'q6_improvement_suggestions', 'q7_contact_me', 'q8_additional_comments',
              'survey_score', 'avg_survey_score', 'owned_by', 'last_modified_date_EST', 'last_modified_date_UTC',
              'last_modified_date_UNIX', 'last_modified_by']

storage_client = storage.Client()
bucket = storage_client.bucket(f"{os.environ['GCS_PREFIX']}_cherwell")
hot_bucket = f"{os.environ['GCS_PREFIX']}_hot_metal"

parser = argparse.ArgumentParser()
parser.add_argument('--input', dest='input', required=True,
                    help='fully specified GCS location from which to retrieve the source json file')
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
         'q4_overall_satisfaction': int, 'created_date_UNIX': int,
         'submitted_date_UNIX': int, 'last_modified_date_UNIX': int,
         'survey_score': float, 'avg_survey_score': float}
df = change_data_type(df, convs)
# df['submitted_date_UNIX'] = df['submitted_date_UNIX'].mask(df['submitted_date_UNIX'] == 0, None)
df['survey_complete'] = df['survey_complete'].map({'True': True, 'False': False})

# convert all different Null types to a single type (None)
df = df.applymap(lambda x: None if isinstance(x, str) and x == '' else x)
df = df.where(df.notnull(), None)
# df = strip_char_pattern(df, ['submitted_date_UNIX'], "(?<=\d)\.0$")
df = df[FINAL_COLS]

#  read in AVRO schema and load into BQ
df.to_gbq("cherwell.customer_satisfaction_survey_responses", project_id=f"{os.environ['GCLOUD_PROJECT']}",
          if_exists="replace")
