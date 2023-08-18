from __future__ import absolute_import

import logging
import os

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.avroio import WriteToAvro

from dataflow_utils import dataflow_utils
from dataflow_utils.dataflow_utils import JsonCoder, SwapFieldNames, generate_args, ChangeDataTypes, FilterFields, \
    StripBeforeDelim, ColumnsCamelToSnakeCase, StandardizeTimes

DEFAULT_DATAFLOW_ARGS = [
    '--save_main_session',
    f"--project={os.environ['GCLOUD_PROJECT']}",
    f"--service_account_email={os.environ['SERVICE_ACCT']}",
    f"--region={os.environ['REGION']}",
    f"--subnetwork={os.environ['SUBNET']}"
]


def run(argv=None):
    # assign the name for the job and specify the AVRO upload location (GCS bucket), arg parser object,
    # and avro schema to validate data with. Return the arg parser values, PipelineOptions, and avro_schemas (dict)

    known_args, pipeline_options, avro_schema = generate_args(
        job_name='intime-assignments-dataflow',
        bucket=f"{os.environ['GCS_PREFIX']}_intime",
        argv=argv,
        schema_name='intime_assignments',
        default_arguments=DEFAULT_DATAFLOW_ARGS,
        limit_workers=[False, None]
    )

    with beam.Pipeline(options=pipeline_options) as p:
        date_fields = ['date']
        times = [('scheduled_start_time', 'EST'), ('scheduled_end_time', 'EST'),
                 ('actual_start_time', 'EST'),  ('actual_end_time', 'EST')]
        field_name_swaps = [('employee_full_name', 'display_name'),
                            ('customer_name', 'court_assignment'),
                            ('location_name', 'location_group'),
                            # ('activity_name', 'district'),
                            # ('rank_name', 'permanent_rank'),
                            ('rank_name', 'rank'),
                            ('unit_name', 'unit'),
                            ('sub_location_name', 'section'),
                            ('scheduled_start_time_UTC', 'scheduled_start'),
                            ('scheduled_end_time_UTC', 'scheduled_end'),
                            ('actual_start_time_UTC', 'actual_start'),
                            ('actual_end_time_UTC', 'actual_end'),
                            ('time_bank_code', 'time_bank_type'),
                            ('date', 'assignment_date')]
        type_changes = [('assignment_id', 'str'), ('employee_id', 'str'),
                        ('hours_sched_min_hours', 'float'), ('time_bank_hours', 'float')]
        drop_fields = ['customer_code', 'location_reference', 'location_code', 'activity_reference',
                       'activity_code', 'sub_location_reference', 'sub_location_code', 'note',
                       'hours_modifier_short_name', 'hours_modifier_reference', 'hours_modifier_code',
                       'hours_actual_minimum', 'time_bank_reference', 'rank_reference', 'unit_reference',
                       'employee_assets', 'time_bank_short_name' 'branch_name', 'branch_reference', 'origin',
                       'scheduled_start_time_EST', 'scheduled_start_time_UNIX',
                       'scheduled_end_time', 'scheduled_end_time_EST', 'scheduled_end_time_UNIX',
                       'actual_start_time', 'actual_start_time_EST', 'actual_start_time_UNIX',
                       'actual_end_time', 'actual_end_time_EST', 'actual_end_time_UNIX']

        lines = p | ReadFromText(known_args.input, coder=JsonCoder())

        load = (
                lines
                | beam.ParDo(StripBeforeDelim(date_fields, delim='T'))
                | beam.ParDo(ColumnsCamelToSnakeCase())
                | beam.ParDo(StandardizeTimes(times, '%Y-%m-%dT%H:%M:%S%z'))
                | beam.ParDo(SwapFieldNames(field_name_swaps))
                | beam.ParDo(ChangeDataTypes(type_changes))
                | beam.ParDo(FilterFields(drop_fields, exclude_target_fields=True))
                | WriteToAvro(known_args.avro_output, schema=avro_schema, file_name_suffix='.avro',
                              use_fastavro=True)
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
