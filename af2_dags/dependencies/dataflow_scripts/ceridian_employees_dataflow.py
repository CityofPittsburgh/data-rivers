from __future__ import absolute_import

import logging
import os
import json

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.avroio import WriteToAvro

from dataflow_utils import dataflow_utils
from dataflow_utils.dataflow_utils import JsonCoder, SwapFieldNames, generate_args, ChangeDataTypes, FilterFields, \
    CrosswalkDeptNames, StripBeforeDelim

DEFAULT_DATAFLOW_ARGS = [
    '--save_main_session',
    f"--project={os.environ['GCLOUD_PROJECT']}",
    f"--service_account_email={os.environ['SERVICE_ACCT']}",
    f"--region={os.environ['REGION']}",
    f"--subnetwork={os.environ['SUBNET']}"
]


class StandardizeEthnicityNames(beam.DoFn):
    def process(self, datum):
        if datum['DFEthnicity_ShortName']:
            datum['DFEthnicity_ShortName'] = datum['DFEthnicity_ShortName'].split(' (')[0]
        else:
            datum['DFEthnicity_ShortName'] = 'Decline to Answer'
        yield datum


def run(argv=None):
    # assign the name for the job and specify the AVRO upload location (GCS bucket), arg parser object,
    # and avro schema to validate data with. Return the arg parser values, PipelineOptions, and avro_schemas (dict)

    known_args, pipeline_options, avro_schema = generate_args(
        job_name='ceridian-employees-dataflow',
        bucket=f"{os.environ['GCS_PREFIX']}_ceridian",
        argv=argv,
        schema_name='ceridian_employees',
        default_arguments=DEFAULT_DATAFLOW_ARGS,
        limit_workers=[False, None]
    )

    with beam.Pipeline(options=pipeline_options) as p:
        strip_fields = ['Employee_HireDate', 'Employee_TerminationDate',
                        'EmployeeEmploymentStatus_CreatedTimestamp', 'Department_ShortName']
        delims = ['T', 'T', 'T', '-']
        before_or_afters = [0, 0, 0, 1]
        field_name_swaps = [('EmployeeEmploymentStatus_EmployeeNumber', 'employee_num'),
                            ('Employee_FirstName', 'first_name'),
                            ('Employee_LastName', 'last_name'),
                            ('Employee_DisplayName', 'display_name'),
                            ('Department_LongName', 'dept_desc'),
                            ('Department_ShortName', 'office'),
                            ('Job_ShortName', 'job_title'),
                            ('Employee_HireDate', 'hire_date'),
                            ('Employee_TerminationDate', 'termination_date'),
                            ('EmployeeEmploymentStatus_CreatedTimestamp', 'account_modified_date'),
                            ('DFUnion_ShortName', 'union'),
                            ('EmploymentStatus_LongName', 'status'),
                            ('PayClass_LongName', 'pay_class'),
                            ('EmployeeManager_ManagerDisplayName', 'manager_name'),
                            ('DFEthnicity_ShortName', 'ethnicity'),
                            ('Employee_Gender', 'gender'),
                            ('SSOLogin', 'sso_login')]
        type_changes = [('employee_num', 'str')]
        drop_fields = ['EmploymentStatus_ShortName', 'DeptJob_ShortName',
                       'Employee_PreferredLastName', 'DenormEmployeeContact_BusinessPhone',
                       'DenormEmployeeContact_HomePhone', 'DenormEmployeeContact_MobilePhone']

        lines = p | ReadFromText(known_args.input, coder=JsonCoder())

        load = (
                lines
                | beam.ParDo(StripBeforeDelim(strip_fields, delims, before_or_afters))
                | beam.ParDo(SwapFieldNames(field_name_swaps))
                | beam.ParDo(ChangeDataTypes(type_changes))
                | beam.ParDo(FilterFields(drop_fields, exclude_target_fields=True))
                | WriteToAvro(known_args.avro_output, schema=avro_schema, file_name_suffix='.avro',
                              use_fastavro=True)
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
