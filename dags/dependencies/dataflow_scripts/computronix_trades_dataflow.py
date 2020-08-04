from __future__ import absolute_import

import argparse
import logging
import os
from datetime import datetime

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.avroio import WriteToAvro
from apache_beam.options.pipeline_options import PipelineOptions

from dataflow_utils import dataflow_utils
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
    dt = datetime.now()
    parser = argparse.ArgumentParser()

    parser.add_argument('--input',
                        dest='input',
                        default='gs://{}_computronix/trades/{}/{}/{}_trades_licenses.json'
                                .format(os.environ['GCS_PREFIX'],
                                        dt.strftime('%Y'),
                                        dt.strftime('%m').lower(),
                                        dt.strftime("%Y-%m-%d")),
                        help='Input file to process.')
    parser.add_argument('--avro_output',
                        dest='avro_output',
                        default='gs://{}_computronix/trades/avro_output/{}/{}/{}/avro_output'
                                .format(os.environ['GCS_PREFIX'],
                                        dt.strftime('%Y'),
                                        dt.strftime('%m').lower(),
                                        dt.strftime("%Y-%m-%d")),
                        help='Output directory to write avro files.')

    known_args, pipeline_args = parser.parse_known_args(argv)

    #TODO: run on on-prem network when route is opened
    # Use runner=DataflowRunner to run in GCP environment, DirectRunner to run locally
    pipeline_args.extend(generate_args('computronix-trades-dataflow_scripts',
                                       '{}_computronix'.format(os.environ['GCS_PREFIX']),
                                       'DataflowRunner'))

    pipeline_args.append('--setup_file={}'.format(os.environ['SETUP_PY_DATAFLOW']))

    avro_schema = get_schema('trade_licenses_computronix')

    pipeline_options = PipelineOptions(pipeline_args)

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
