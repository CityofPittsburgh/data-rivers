from __future__ import absolute_import

import logging
import os

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.avroio import WriteToAvro

from dataflow_utils import dataflow_utils
from dataflow_utils.dataflow_utils import JsonCoder, SwapFieldNames, generate_args, ChangeDataTypes, FilterFields, \
    StripBeforeDelim, ColumnsCamelToSnakeCase

DEFAULT_DATAFLOW_ARGS = [
    '--save_main_session',
    f"--project={os.environ['GCLOUD_PROJECT']}",
    f"--service_account_email={os.environ['SERVICE_ACCT']}",
    f"--region={os.environ['REGION']}",
    f"--subnetwork={os.environ['SUBNET']}"
]


class ReplaceChar(beam.DoFn):
    def __init__(self, fields, swap_chars):
        """
        :param field: lis of string field names that will have a character replaced
        :param swap_chars: tuple that contains a character to replaced and a character that will replace it
        """
        self.fields = fields
        self.swap_chars = swap_chars

    def process(self, datum):
        for field in self.fields:
            if datum[field]:
                datum[field] = datum[field].replace(self.swap_chars[0], self.swap_chars[1])
        yield datum


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
        swap_char_fields = ['scheduled_start_time', 'scheduled_end_time', 'actual_start_time', 'actual_end_time']
        field_name_swaps = [('employee_full_name', 'display_name'),
                            ('customer_name', 'court_assignment'),
                            ('location_name', 'location_group'),
                            ('rank_name', 'permanent_rank'),
                            ('unit_name', 'unit'),
                            ('sub_location_name', 'section'),
                            ('time_bank_code', 'time_bank_type'),
                            ('date', 'assignment_date')]
        type_changes = [('assignment_id', 'str'), ('employee_id', 'str'),
                        ('hours_sched_min_hours', 'float'), ('time_bank_hours', 'float')]
        drop_fields = ['customer_code', 'location_reference', 'location_code', 'activity_reference',
                       'activity_code', 'sub_location_reference', 'sub_location_code', 'note',
                       'hours_modifier_short_name', 'hours_modifier_reference', 'hours_modifier_code',
                       'hours_actual_minimum', 'time_bank_reference', 'rank_reference', 'unit_reference',
                       'employee_assets', 'time_bank_short_name' 'branch_name', 'branch_reference', 'origin']

        lines = p | ReadFromText(known_args.input, coder=JsonCoder())

        load = (
                lines
                | beam.ParDo(StripBeforeDelim(date_fields, delim='T'))
                | beam.ParDo(ColumnsCamelToSnakeCase())
                | beam.ParDo(ReplaceChar(swap_char_fields, ('T', ' ')))
                | beam.ParDo(SwapFieldNames(field_name_swaps))
                | beam.ParDo(ChangeDataTypes(type_changes))
                | beam.ParDo(FilterFields(drop_fields, exclude_target_fields=True))
                | WriteToAvro(known_args.avro_output, schema=avro_schema, file_name_suffix='.avro',
                              use_fastavro=True)
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
