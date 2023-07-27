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

USER_DEFINED_CONST_BUCKET = "user_defined_data"


class CrosswalkDeptNames(beam.DoFn):
    def __init__(self, place_id_bucket, crosswalk_file):
        """
        :param place_id_bucket - Name of GCS bucket the crosswalk file will be extracted from
        :param crosswalk_file - Environmental variable containing the name of a JSON file that maps department names
        returned by the Ceridian API to department names sourced from the City's Department of Human Resources
        """
        self.place_id_bucket = place_id_bucket
        self.crosswalk_file = crosswalk_file

    def process(self, datum):
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(self.place_id_bucket)
        blob = bucket.get_blob(self.crosswalk_file)
        cw = blob.download_as_string()
        crosswalk = json.loads(cw.decode('utf-8'))
        datum['dept'] = None
        datum['dept_desc'] = None
        datum['office'] = None
        datum['corporation'] = None
        for dict in crosswalk:
            if datum['org_unit'] == dict['Ceridian Department Name']:
                datum['dept'] = dict['Department']
                datum['dept_desc'] = dict['Department Description']
                datum['office'] = dict['Office']
                datum['corporation'] = dict['Corporation']
        if not datum['dept']:
            split_dept = datum['org_unit'].split("-")
            datum['dept'] = split_dept[0]
            try:
                datum['dept_desc'] = split_dept[1]
                try:
                    datum['office'] = split_dept[2]
                except:
                    datum['office'] = split_dept[0]
            except:
                datum['dept_desc'] = split_dept[0]
            datum['corporation'] = 'City of Pittsburgh'
        yield datum


class StripDate(beam.DoFn):
    def process(self, datum):
        if datum['EmployeePaySummary_BusinessDate']:
            datum['EmployeePaySummary_BusinessDate'] = datum['EmployeePaySummary_BusinessDate'].split('T')[0]
        yield datum


def run(argv=None):
    # assign the name for the job and specify the AVRO upload location (GCS bucket), arg parser object,
    # and avro schema to validate data with. Return the arg parser values, PipelineOptions, and avro_schemas (dict)

    known_args, pipeline_options, avro_schema = generate_args(
        job_name='ceridian-dataflow',
        bucket=f"{os.environ['GCS_PREFIX']}_ceridian",
        argv=argv,
        schema_name='ceridian_timekeeping',
        default_arguments=DEFAULT_DATAFLOW_ARGS,
        limit_workers=[False, None],
        backfill_dag=False,
        use_df_runner=True
    )

    with beam.Pipeline(options=pipeline_options) as p:
        field_name_swaps = [('EmployeeEmploymentStatus_EmployeeNumber', 'employee_num'),
                            ('Employee_DisplayName', 'display_name'),
                            ('OrgUnit_ShortName', 'org_unit'),
                            ('Job_ShortName', 'job_title'),
                            ('EmployeePaySummary_BusinessDate', 'pay_period'),
                            ('PayAdjCode_ShortName', 'pay_code'),
                            ('PayCategory_CodeName', 'pay_category'),
                            ('PayCategory_LongName', 'pay_category_description'),
                            ('EmployeePaySummary_NetHoursSum', 'net_hours'),
                            ('LaborMetricsCode0_XRefCode', 'labor_metrics_code')]
        type_changes = [('employee_num', 'str'), ('net_hours', 'float')]
        drop_fields = ['EmployeePaySummary_PayAmountSum', 'EmployeePaySummary_Rate', 'LaborMetricsCode0_LedgerCode']

        lines = p | ReadFromText(known_args.input, coder=JsonCoder())

        load = (
                lines
                | beam.ParDo(StripDate())
                | beam.ParDo(SwapFieldNames(field_name_swaps))
                | beam.ParDo(CrosswalkDeptNames(USER_DEFINED_CONST_BUCKET, os.environ['CERIDIAN_DEPT_FILE']))
                | beam.ParDo(ChangeDataTypes(type_changes))
                | beam.ParDo(FilterFields(drop_fields, exclude_target_fields=True))
                | WriteToAvro(known_args.avro_output, schema=avro_schema, file_name_suffix='.avro',
                              use_fastavro=True)
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
