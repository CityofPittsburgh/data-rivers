from __future__ import absolute_import

import logging
import os

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.avroio import WriteToAvro

from dataflow_utils.dataflow_utils import generate_args, get_schema, JsonCoder


class FormatColumnNames(beam.DoFn):
    def process(self, datum):
        cleaned_col_names = {
            'LICENSENUMBER': u'license_number',
            'LICENSETYPENAME': u'license_type_name',
            'NAICSCODE': u'naics_code',
            'BUSINESSNAME': u'business_name',
            'LICENSESTATE': u'license_state',
            'INITIALISSUEDATE': u'initial_issue_date',
            'MOSTRECENTISSUEDATE': u'most_recent_issue_date',
            'EFFECTIVEDATE': u'effective_date',
            'EXPIRATIONDATE': u'expiration_date'
        }
        formatted = dict((cleaned_col_names[key], value) for (key, value) in datum.items())
        yield formatted


class ConvertTypes(beam.DoFn):
    def process(self, datum):
        datum['naics_code'] = int(datum['naics_code'])
        yield datum


def run(argv=None):
    known_args, pipeline_options, avro_schema = generate_args(
        job_name='computronix-trades-dataflow',
        bucket='{}_computronix'.format(os.environ['GCS_PREFIX']),
        argv=argv,
        schema_name='trade_licenses_computronix'
    )

    with beam.Pipeline(options=pipeline_options) as p:
        # Read the text file[pattern] into a PCollection.
        lines = p | ReadFromText(known_args.input, coder=JsonCoder())

        load = (
                lines
                | beam.ParDo(FormatColumnNames())
                | beam.ParDo(ConvertTypes())
                | WriteToAvro(known_args.avro_output, schema=avro_schema, file_name_suffix='.avro', use_fastavro=True))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
