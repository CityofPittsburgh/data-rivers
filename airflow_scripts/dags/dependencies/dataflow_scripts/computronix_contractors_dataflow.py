from __future__ import absolute_import

import argparse
import json
import logging
import os

import apache_beam as beam
import avro
import fastavro

from apache_beam.io import ReadFromText
from apache_beam.io.avroio import WriteToAvro
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from avro import schema

from datetime import datetime
from google.cloud import storage
from airflow_scripts.dags.dependencies.dataflow_scripts.dataflow_utils import hash_func, download_schema, \
    generate_args, JsonCoder


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
                        default='gs://pghpa_computronix/contractors/{}/{}/{}_contractors_licenses.json'.format(dt.strftime('%Y'),
                                                                                          dt.strftime('%m').lower(),
                                                                                          dt.strftime("%Y-%m-%d")),
                        help='Input file to process.')
    parser.add_argument('--avro_output',
                        dest='avro_output',
                        default='gs://pghpa_computronix/contractors/avro_output/{}/{}/{}/avro_output'.format(dt.strftime('%Y'),
                                                                                         dt.strftime('%m').lower(),
                                                                                         dt.strftime("%Y-%m-%d")),
                        help='Output directory to write avro files.')

    known_args, pipeline_args = parser.parse_known_args(argv)

    #TODO: run on on-prem network when route is opened
    # Use runner=DataflowRunner to run in GCP environment, DirectRunner to run locally
    pipeline_args.extend(generate_args('computronix-trades-dataflow_scripts', 'pghpa_computronix', 'DirectRunner'))

    schema.RecordSchema.__hash__ = hash_func

    download_schema('pghpa_avro_schemas', 'contractors_computronix.avsc', 'contractors_computronix.avsc')

    SCHEMA_PATH = 'contractors_computronix.avsc'
    # fastavro does the work of avro.schema.parse(), just need to pass dict
    avro_schema = json.loads(open(SCHEMA_PATH).read())

    pipeline_options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=pipeline_options) as p:
        # Read the text file[pattern] into a PCollection.
        lines = p | ReadFromText(known_args.input, coder=JsonCoder())

        load = (
                lines
                | beam.ParDo(FormatColumnNames())
                | beam.ParDo(ConvertTypes())
                | beam.io.avroio.WriteToAvro(known_args.avro_output, schema=avro_schema, file_name_suffix='.avro', use_fastavro=True))

    os.remove('contractors_computronix.avsc')


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
