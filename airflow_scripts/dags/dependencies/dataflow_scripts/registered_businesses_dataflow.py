from __future__ import absolute_import

import argparse
import logging
import os

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.avroio import WriteToAvro

from dataflow_utils import dataflow_utils
from dataflow_utils.dataflow_utils import get_schema, clean_csv_int, clean_csv_string, generate_args, normalize_address


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
        datum['normalized_address'] = normalize_address(datum['address_full'])
        return datum


def run(argv=None):

    known_args, pipeline_options, avro_schema = generate_args(
        job_name='registered-businesses-dataflow',
        bucket='{}_finance'.format(os.environ['GCS_PREFIX']),
        argv=argv,
        schema_name='registered_businesses',
        runner='DataflowRunner'
    )

    with beam.Pipeline(options=pipeline_options) as p:
        # Read the text file[pattern] into a PCollection.
        lines = p | ReadFromText(known_args.input, skip_header_lines=1)

        load = (
                lines
                | beam.ParDo(ConvertToDicts())
                | beam.ParDo(AddNormalizedAddress())
                | WriteToAvro(known_args.avro_output, schema=avro_schema, file_name_suffix='.avro', use_fastavro=True))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
