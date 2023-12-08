from __future__ import absolute_import

import logging
import os

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.avroio import WriteToAvro

from dataflow_utils import dataflow_utils
from dataflow_utils.dataflow_utils import JsonCoder, SwapFieldNames, generate_args, ChangeDataTypes, FilterFields, \
    StripBeforeDelim

DEFAULT_DATAFLOW_ARGS = [
    '--save_main_session',
    f"--project={os.environ['GCLOUD_PROJECT']}",
    f"--service_account_email={os.environ['SERVICE_ACCT']}",
    f"--region={os.environ['REGION']}",
    f"--subnetwork={os.environ['SUBNET']}"
]


class DictToHashable:
    def __call__(self, dictionary):
        return frozenset(dictionary.items())


class HashableToDict:
    def __call__(self, hashable):
        return dict((key, value) for key, value in hashable)


def run(argv=None):
    # assign the name for the job and specify the AVRO upload location (GCS bucket), arg parser object,
    # and avro schema to validate data with. Return the arg parser values, PipelineOptions, and avro_schemas (dict)

    known_args, pipeline_options, avro_schema = generate_args(
        job_name='ceridian-job-codes-dataflow',
        bucket=f"{os.environ['GCS_PREFIX']}_ceridian",
        argv=argv,
        schema_name='ceridian_jobs',
        default_arguments=DEFAULT_DATAFLOW_ARGS,
        limit_workers=[False, None]
    )

    with beam.Pipeline(options=pipeline_options) as p:
        strip_fields = ['Job_JobUDFString1', 'JobFunction_ShortName']
        delims = [':', ':']
        before_or_afters = [1, 1]
        field_name_swaps = [('Job_ShortName', 'job_title'),
                            ('Job_JobUDFString1', 'eeo4_category'),
                            ('JobFunction_ShortName', 'job_function'),
                            ('FLSAStatus_ShortName', 'flsa_status'),
                            ('JobFamily_ShortName', 'classification'),
                            ('Job_IsUnionJob', 'is_union_job'),
                            ('DFUnion_ShortName', 'union_name')]
        type_changes = [('is_union_job', 'bool'), ('eeo4_category', 'nullstr')]
        drop_fields = ['Job_XrefCode', 'PRWCBCode_WCBCode', 'test', 'Job_LongName',
                       'Job_JobUDFString2', 'Job_JobUDFString3']

        lines = p | ReadFromText(known_args.input, coder=JsonCoder())

        load = (
                lines
                | beam.ParDo(StripBeforeDelim(strip_fields, delims, before_or_afters))
                | beam.ParDo(SwapFieldNames(field_name_swaps))
                | beam.ParDo(ChangeDataTypes(type_changes))
                | beam.ParDo(FilterFields(drop_fields, exclude_target_fields=True))
                # Convert dictionaries to hashable objects so they can be de-duplicated
                | 'ToHashable' >> beam.Map(DictToHashable())
                | beam.Distinct()
                # Convert PCollection back to dictionaries so it can be written to Avro
                | 'ToDict' >> beam.Map(HashableToDict())
                | WriteToAvro(known_args.avro_output, schema=avro_schema, file_name_suffix='.avro',
                              use_fastavro=True)
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
