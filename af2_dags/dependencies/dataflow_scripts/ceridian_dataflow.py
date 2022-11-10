from __future__ import absolute_import

import logging
import os
import json

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.avroio import WriteToAvro

from dataflow_utils import dataflow_utils
from dataflow_utils.dataflow_utils import JsonCoder, SwapFieldNames, generate_args, ChangeDataTypes, FilterFields
from google.cloud import storage

DEFAULT_DATAFLOW_ARGS = [
        '--save_main_session',
        f"--project={os.environ['GCLOUD_PROJECT']}",
        f"--service_account_email={os.environ['SERVICE_ACCT']}",
        f"--region={os.environ['REGION']}",
        f"--subnetwork={os.environ['SUBNET']}"
]


class CrosswalkDeptNames(beam.DoFn):
    def __init__(self, gcs_prefix):
        """
        :param gcs_prefix - Environmental variable denoting the GCS project prefix string. DAG will crash if this
                            isn't included as a parameter, as it can not be obtained from within the class.
        """
        self.gcs_prefix = gcs_prefix
    def process(self, datum):
        storage_client = storage.Client()
        bucket = storage_client.bucket(f"{self.gcs_prefix}_ceridian")
        blob = bucket.get_blob("organizational_structure.json")
        cw = blob.download_as_string()
        crosswalk = json.loads(cw.decode('utf-8'))
        datum['bureau'] = ''
        datum['region'] = ''
        datum['corporation'] = ''
        for dict in crosswalk:
            if datum['department'] == dict['Department']:
                datum['bureau'] = dict['Bureau']
                datum['region'] = dict['Region']
                datum['corporation'] = dict['Corporation']
        yield datum


class StandardizeEthnicityNames(beam.DoFn):
    def process(self, datum):
        if datum['DFEthnicity_ShortName']:
            datum['DFEthnicity_ShortName'] = datum['DFEthnicity_ShortName'].split(' (')[0]
        else:
            datum['DFEthnicity_ShortName'] = 'Decline to Answer'
        yield datum


class StripDate(beam.DoFn):
    def process(self, datum):
        if datum['Employee_HireDate']:
            datum['Employee_HireDate'] = datum['Employee_HireDate'].split('T')[0]
        yield datum


def run(argv = None):
    # assign the name for the job and specify the AVRO upload location (GCS bucket), arg parser object,
    # and avro schema to validate data with. Return the arg parser values, PipelineOptions, and avro_schemas (dict)

    known_args, pipeline_options, avro_schema = generate_args(
            job_name = 'ceridian-dataflow',
            bucket = f"{os.environ['GCS_PREFIX']}_ceridian",
            argv = argv,
            schema_name = 'ceridian_employees',
            default_arguments = DEFAULT_DATAFLOW_ARGS,
            limit_workers = [False, None]
    )

    with beam.Pipeline(options = pipeline_options) as p:
        field_name_swaps = [('EmployeeEmploymentStatus_EmployeeNumber', 'employee_num'),
                            ('Employee_FirstName', 'first_name'),
                            ('Employee_LastName', 'last_name'),
                            ('Employee_DisplayName', 'display_name'),
                            ('Employee_PreferredLastName', 'preferred_name'),
                            ('Department_ShortName', 'department'),
                            ('Job_ShortName', 'job_title'),
                            ('Employee_HireDate', 'hire_date'),
                            ('DFUnion_ShortName', 'union'),
                            ('EmploymentStatus_LongName', 'status'),
                            ('PayClass_LongName', 'pay_class'),
                            ('EmployeeManager_ManagerDisplayName', 'manager_name'),
                            ('DFEthnicity_ShortName', 'ethnicity'),
                            ('Employee_Gender', 'gender'),
                            ('DenormEmployeeContact_BusinessPhone', 'work_phone'),
                            ('DenormEmployeeContact_HomePhone', 'home_phone'),
                            ('DenormEmployeeContact_MobilePhone', 'mobile_phone')]
        type_changes = [('employee_num', 'str')]
        drop_fields = ['EmploymentStatus_ShortName', 'DeptJob_ShortName', 'Department_LongName']

        lines = p | ReadFromText(known_args.input, coder = JsonCoder())

        load = (
                lines
                | beam.ParDo(StripDate())
                | beam.ParDo(StandardizeEthnicityNames())
                | beam.ParDo(SwapFieldNames(field_name_swaps))
                | beam.ParDo(CrosswalkDeptNames(os.environ['GCS_PREFIX']))
                | beam.ParDo(ChangeDataTypes(type_changes))
                | beam.ParDo(FilterFields(drop_fields, exclude_target_fields=True))
                | WriteToAvro(known_args.avro_output, schema = avro_schema, file_name_suffix = '.avro',
                              use_fastavro = True)
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()