from __future__ import absolute_import

import logging
import os

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.avroio import WriteToAvro

from dataflow_utils import dataflow_utils
from dataflow_utils.dataflow_utils import JsonCoder, generate_args, ChangeDataTypes, ExtractFieldWithComplexity

DEFAULT_DATAFLOW_ARGS = [
        '--save_main_session',
        f"--project={os.environ['GCLOUD_PROJECT']}",
        f"--service_account_email={os.environ['SERVICE_ACCT']}",
        f"--region={os.environ['REGION']}",
        f"--subnetwork={os.environ['SUBNET']}"
]

def run(argv = None):
    # assign the name for the job and specify the AVRO upload location (GCS bucket), arg parser object,
    # and avro schema to validate data with. Return the arg parser values, PipelineOptions, and avro_schemas (dict)

    known_args, pipeline_options, avro_schema = generate_args(
            job_name = 'cherwell-dataflow',
            bucket = f"{os.environ['GCS_PREFIX']}_cherwell",
            argv = argv,
            schema_name = 'cherwell_incidents',
            default_arguments = DEFAULT_DATAFLOW_ARGS,
            limit_workers = [False, None]
    )

    with beam.Pipeline(options = pipeline_options) as p:
        source_fields = ['fields', 'fields', 'fields', 'fields', 'fields', 'fields', 'fields', 'fields',
                         'fields', 'fields', 'fields', 'fields', 'fields', 'fields', 'fields', 'fields']
        nested_fields = ['value', 'value', 'value', 'value', 'value', 'value', 'value', 'value',
                         'value', 'value', 'value', 'value', 'value', 'value', 'value', 'value']
        new_field_names = ['id', 'created_date', 'status', 'service', 'category', 'subcategory', 'description',
                           'priority', 'last_modified_date', 'closed_date', 'assigned_team', 'assigned_to',
                           'incident_type', 'call_source', 'responded_date', 'requester_department']
        search_fields = [{'name': 'IncidentID'}, {'name': 'CreatedDateTime'}, {'name': 'Status'}, {'name': 'Service'},
                         {'name': 'Category'}, {'name': 'Subcategory'}, {'name': 'Description'}, {'name': 'Priority'},
                         {'name': 'LastModifiedDateTime'}, {'name': 'ClosedDateTime'}, {'name': 'AssignedTeam'},
                         {'name': 'AssignedTo'}, {'name': 'IncidentType'}, {'name': 'CallSource'},
                         {'name': 'Stat_DateTimeResponded'}, {'name': 'RequesterDepartment'}]
        type_changes = [('id', 'str'), ('priority', 'str')]

        lines = p | ReadFromText(known_args.input, coder = JsonCoder())

        load = (
                lines
                | beam.ParDo(ExtractFieldWithComplexity(source_fields, nested_fields, new_field_names, search_fields))
                | beam.ParDo(ChangeDataTypes(type_changes))
                | WriteToAvro(known_args.avro_output, schema = avro_schema, file_name_suffix = '.avro',
                              use_fastavro = True)
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
