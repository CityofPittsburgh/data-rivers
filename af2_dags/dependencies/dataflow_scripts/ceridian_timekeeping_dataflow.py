from __future__ import absolute_import

import logging
import os
import json

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.avroio import WriteToAvro

from dataflow_utils import dataflow_utils
from dataflow_utils.dataflow_utils import JsonCoder, SwapFieldNames, generate_args, ChangeDataTypes, FilterFields, \
    CrosswalkDeptNames, StripBeforeDelim, StandardizeTimes

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
        job_name='ceridian-timekeeping-dataflow',
        bucket=f"{os.environ['GCS_PREFIX']}_ceridian",
        argv=argv,
        schema_name='ceridian_timekeeping',
        default_arguments=DEFAULT_DATAFLOW_ARGS,
        limit_workers=[False, None]
    )

    with beam.Pipeline(options=pipeline_options) as p:
        date_fields = ['EmployeePaySummary_BusinessDate']
        times = [(date_fields[0], 'US/Eastern')]
        field_name_swaps = [('EmployeeEmploymentStatus_EmployeeNumber', 'employee_num'),
                            ('Employee_DisplayName', 'display_name'),
                            ('Department_LongName', 'dept_desc'),
                            ('Job_ShortName', 'job_title'),
                            ('EmployeePaySummary_BusinessDate_EST', 'work_period'),
                            ('PayAdjCode_ShortName', 'pay_code'),
                            ('PayCategory_CodeName', 'pay_category'),
                            ('PayCategory_LongName', 'pay_category_description'),
                            ('EmployeePaySummary_NetHoursSum', 'net_hours'),
                            ('LaborMetricsCode0_XRefCode', 'labor_metrics_code')]
        type_changes = [('employee_num', 'str'), ('net_hours', 'float')]
        drop_fields = ['EmployeePaySummary_PayAmountSum', 'EmployeePaySummary_Rate', 'OrgUnit_ShortName',
                       'LaborMetricsCode0_LedgerCode', 'Department_LongName', 'EmployeePaySummary_BusinessDate_UTC',
                       'EmployeePaySummary_BusinessDate_UNIX']

        lines = p | ReadFromText(known_args.input, coder=JsonCoder())

        load = (
                lines
                | beam.ParDo(StripBeforeDelim(date_fields, delim=['T'], before_or_after=[0]))
                | beam.ParDo(StandardizeTimes(times, "%m/%d/%Y"))
                | beam.ParDo(SwapFieldNames(field_name_swaps))
                | beam.ParDo(ChangeDataTypes(type_changes))
                | beam.ParDo(FilterFields(drop_fields, exclude_target_fields=True))
                | WriteToAvro(known_args.avro_output, schema=avro_schema, file_name_suffix='.avro',
                              use_fastavro=True)
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
