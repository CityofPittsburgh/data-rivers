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
from dataflow_utils import dataflow_utils
from dataflow_utils.dataflow_utils import hash_func, download_schema, clean_csv_int, clean_csv_string, generate_args


class ConvertToDicts(beam.DoFn):
    def process(self, datum):

        acct_no, name, trade_name, desc_of_business, business_type, address_type, city, state, zip, data_created, \
        business_start_date_in_pgh, address1, address2 = datum.split(',')

        address_full = clean_csv_string(address1) + ' ' + clean_csv_string(address2)

        return [{
            'acct_no': clean_csv_string(acct_no),
            'name': clean_csv_string(name),
            'trade_name': clean_csv_string(trade_name),
            'desc_of_business': clean_csv_string(desc_of_business),
            'business_type': clean_csv_string(business_type),
            'address_type': clean_csv_string(address_type),
            'city': clean_csv_string(city),
            'state': clean_csv_string(state),
            'zip': clean_csv_int(zip),
            'date_created': clean_csv_string(data_created),
            'business_start_date_in_pgh': clean_csv_string(business_start_date_in_pgh),
            'address_full': address_full
        }]


class AddNormalizedAddress(beam.DoFn):
    def process(self, datum):
        datum['normalized_address'] = normalize_address_record(datum['address_full'])
        return datum


def run(argv=None):
    dt = datetime.now()
    parser = argparse.ArgumentParser()

    parser.add_argument('--input',
                        dest='input',
                        default='gs://{}_finance/{}/{}/{}_registered_businesses.csv'
                                .format(os.environ['GCS_PREFIX'],
                                        dt.strftime('%Y'),
                                        dt.strftime('%m').lower(),
                                        dt.strftime("%Y-%m-%d")),
                        help='Input file to process.')
    parser.add_argument('--avro_output',
                        dest='avro_output',
                        default='gs://{}_finance/avro_output/{}/{}/{}/avro_output'
                                .format(os.environ['GCS_PREFIX'],
                                        dt.strftime('%Y'),
                                        dt.strftime('%m').lower(),
                                        dt.strftime("%Y-%m-%d")),
                        help='Output directory to write avro files.')

    known_args, pipeline_args = parser.parse_known_args(argv)

    #TODO: run on on-prem network when route is opened

    # Use runner=DataflowRunner to run in GCP environment, DirectRunner to run locally
    pipeline_args.extend(generate_args('registered-businesses-dataflow_scripts',
                                       '{}_finance'.format(os.environ['GCS_PREFIX']),
                                       'DirectRunner'))

    schema.RecordSchema.__hash__ = hash_func

    download_schema('pghpa_avro_schemas', 'registered_businesses.avsc', 'registered_businesses.avsc')

    SCHEMA_PATH = 'registered_businesses.avsc'
    avro_schema = json.loads(open(SCHEMA_PATH).read())

    pipeline_options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=pipeline_options) as p:
        # Read the text file[pattern] into a PCollection.
        lines = p | ReadFromText(known_args.input, skip_header_lines=1)

        load = (
                lines
                | beam.ParDo(ConvertToDicts())
                # | beam.ParDo(AddNormalizedAddress())
                # TODO: ^add this step once we are on python3
                | beam.io.avroio.WriteToAvro(known_args.avro_output, schema=avro_schema, file_name_suffix='.avro', use_fastavro=True))

    os.remove('registered_businesses.avsc')


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
