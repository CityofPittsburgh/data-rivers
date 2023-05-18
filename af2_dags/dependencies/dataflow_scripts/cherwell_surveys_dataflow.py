from __future__ import absolute_import

import logging
import os

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.avroio import WriteToAvro

from dataflow_utils import dataflow_utils
from dataflow_utils.dataflow_utils import JsonCoder, generate_args, ExtractFieldWithComplexity, StandardizeTimes, \
    ChangeDataTypes

DEFAULT_DATAFLOW_ARGS = [
    '--save_main_session',
    f"--project={os.environ['GCLOUD_PROJECT']}",
    f"--service_account_email={os.environ['SERVICE_ACCT']}",
    f"--region={os.environ['REGION']}",
    f"--subnetwork={os.environ['SUBNET']}"
]

FIELD_COUNT = 26


def run(argv=None):
    # assign the name for the job and specify the AVRO upload location (GCS bucket), arg parser object,
    # and avro schema to validate data with. Return the arg parser values, PipelineOptions, and avro_schemas (dict)

    known_args, pipeline_options, avro_schema = generate_args(
        job_name='cherwell-surveys-dataflow',
        bucket=f"{os.environ['GCS_PREFIX']}_cherwell",
        argv=argv,
        schema_name='cherwell_surveys',
        default_arguments=DEFAULT_DATAFLOW_ARGS,
        limit_workers=[False, None]
    )

    with beam.Pipeline(options=pipeline_options) as p:
        source_fields = ['fields'] * FIELD_COUNT
        nested_fields = ['value'] * FIELD_COUNT
        new_field_names = ['id', 'incident_id', 'created_date', 'submitted_by', 'submitted_date', 'survey_complete',
                           'q1_timely_resolution', 'q2_handled_professionally', 'q3_clear_communication',
                           'q4_overall_satisfaction', 'q5_request_handled_first_time', 'q6_improvement_suggestions',
                           'q7_contact_me', 'q8_additional_comments', 'survey_score', 'avg_survey_score',
                           'owned_by', 'last_modified_date', 'last_modified_by']
        add_nested_fields = [''] * FIELD_COUNT
        search_fields = [{'name': 'SurveyID'}, {'name': 'IncidentID'}, {'name': 'CreatedDateTime'},
                         {'name': 'SurveySubmittedBy'}, {'name': 'SurveySubmittedOn'}, {'name': 'SurveyComplete'},
                         {'name': 'Q1'}, {'name': 'Q2'}, {'name': 'Q3'}, {'name': 'Q4'}, {'name': 'Q5'}, {'name': 'Q6'},
                         {'name': 'Q7'}, {'name': 'Q8'}, {'name': 'SurveyScore'}, {'name': 'AvgSurveyScore'},
                         {'name': 'OwnedByTechnician'}, {'name': 'LastModDateTime'}, {'name': 'LastModBy'}]
        add_search_vals = [''] * FIELD_COUNT
        times = [('created_date', 'US/Eastern'), ('submitted_date', 'US/Eastern'), ('last_modified_date', 'US/Eastern')]
        type_changes = [('id', 'str'), ('incident_id', 'str'), ('q1_timely_resolution', 'int'),
                        ('q2_handled_professionally', 'int'), ('q3_clear_communication', 'int'),
                        ('q4_overall_satisfaction', 'int'), ('survey_complete', 'bool'),
                        ('survey_score', 'float'), ('avg_survey_score', 'float')]

        lines = p | ReadFromText(known_args.input, coder=JsonCoder())

        load = (
                lines
                | beam.ParDo(ExtractFieldWithComplexity(source_fields, nested_fields, new_field_names,
                                                        add_nested_fields, search_fields, add_search_vals))
                | beam.ParDo(StandardizeTimes(times, "%m/%d/%Y %I:%M:%S"))
                | beam.ParDo(ChangeDataTypes(type_changes))
                | WriteToAvro(known_args.avro_output, schema=avro_schema, file_name_suffix='.avro',
                              use_fastavro=True)
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
