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

FIELD_COUNT = 29


def run(argv=None):
    # assign the name for the job and specify the AVRO upload location (GCS bucket), arg parser object,
    # and avro schema to validate data with. Return the arg parser values, PipelineOptions, and avro_schemas (dict)

    known_args, pipeline_options, avro_schema = generate_args(
        job_name='cherwell-dataflow',
        bucket=f"{os.environ['GCS_PREFIX']}_cherwell",
        argv=argv,
        schema_name='cherwell_incidents',
        default_arguments=DEFAULT_DATAFLOW_ARGS,
        limit_workers=[False, None]
    )

    with beam.Pipeline(options=pipeline_options) as p:
        source_fields = ['fields'] * FIELD_COUNT
        nested_fields = ['value'] * FIELD_COUNT
        new_field_names = ['id', 'created_date', 'status', 'service', 'category', 'subcategory', 'description',
                           'priority', 'last_modified_date', 'closed_date', 'assigned_team', 'assigned_to',
                           'assigned_to_manager', 'incident_type', 'respond_by_deadline', 'resolve_by_deadline',
                           'call_source', 'incident_reopened', 'responded_date', 'resolved_date', 'number_of_touches',
                           'number_of_escalations', 'requester_department', 'requester', 'on_behalf_of', 'customer',
                           'initial_assigned_team', 'comments', 'close_description']
        add_nested_fields = [''] * FIELD_COUNT
        search_fields = [{'name': 'IncidentID'}, {'name': 'CreatedDateTime'}, {'name': 'Status'}, {'name': 'Service'},
                         {'name': 'Category'}, {'name': 'Subcategory'}, {'name': 'Description'}, {'name': 'Priority'},
                         {'name': 'LastModifiedDateTime'}, {'name': 'ClosedDateTime'}, {'name': 'AssignedTeam'},
                         {'name': 'AssignedTo'}, {'name': 'AssignedToManager'}, {'name': 'IncidentType'},
                         {'name': 'SLARespondByDeadline'}, {'name': 'SLAResolveByDeadline'}, {'name': 'CallSource'},
                         {'name': 'Stat_IncidentReopened'}, {'name': 'Stat_DateTimeResponded'},
                         {'name': 'Stat_DateTimeResolved'}, {'name': 'Stat_NumberOfTouches'},
                         {'name': 'Stat_NumberOfEscalations'}, {'name': 'RequesterDepartment'},
                         {'name': 'Requester'}, {'name': 'OnBehalfOf'}, {'name': 'CustomerDisplayName'},
                         {'name': 'InitialAssignedTeam'}, {'name': 'Comments'}, {'name': 'CloseDescription'}]
        add_search_vals = [''] * FIELD_COUNT
        times = [('created_date', 'US/Eastern'), ('last_modified_date', 'US/Eastern'),
                 ('closed_date', 'US/Eastern'), ('responded_date', 'US/Eastern'),
                 ('resolved_date', 'US/Eastern'), ('respond_by_deadline', 'US/Eastern'),
                 ('resolve_by_deadline', 'US/Eastern')]
        type_changes = [('assigned_team', 'nullstr'), ('assigned_to', 'nullstr'), ('assigned_to_manager', 'nullstr'),
                        ('incident_type', 'nullstr'), ('incident_reopened', 'bool'), ('requester_department', 'nullstr'),
                        ('requester', 'nullstr'), ('on_behalf_of', 'bool'), ('customer', 'nullstr'),
                        ('comments', 'nullstr'), ('close_description', 'nullstr'), ('initial_assigned_team', 'nullstr')]

        lines = p | ReadFromText(known_args.input, coder=JsonCoder())

        load = (
                lines
                | beam.ParDo(ExtractFieldWithComplexity(source_fields, nested_fields, new_field_names,
                                                        add_nested_fields, search_fields, add_search_vals))
                | beam.ParDo(StandardizeTimes(times, "%m/%d/%Y %I:%M:%S %p"))
                | beam.ParDo(ChangeDataTypes(type_changes))
                | WriteToAvro(known_args.avro_output, schema=avro_schema, file_name_suffix='.avro',
                              use_fastavro=True)
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
