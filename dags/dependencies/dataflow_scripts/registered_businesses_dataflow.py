from __future__ import absolute_import

import os
import logging

import apache_beam as beam

from abc import ABC
from apache_beam.io import ReadFromText
from apache_beam.io.avroio import WriteToAvro
from apache_beam.options.value_provider import StaticValueProvider

from dataflow_utils import dataflow_utils
from dataflow_utils.dataflow_utils import generate_args, ColumnsToLowerCase, NormalizeAddress, JsonCoder


class ParseAddress(beam.DoFn):
    def process(self, datum):
        address_components = ['street_number', 'street_name', 'street_suffix', 'apt_no', 'misc_address_line', 'city',
                              'state_code', 'zip']

        datum['address_full'] = ''
        for component in address_components:
            if datum[component]:
                try:
                    if component == 'street_number':
                        datum[component] = int(datum[component])

                    if component == 'zip':
                        datum['address_full'] += datum[component][0:5]
                    else:
                        datum['address_full'] += (str(datum[component]) + ' ')
                except ValueError:
                    pass

            del datum[component]

        yield datum


def run(argv=None):

    known_args, pipeline_options, avro_schema = generate_args(
        job_name='registered-businesses-dataflow',
        bucket='{}_finance'.format(os.environ['GCS_PREFIX']),
        argv=argv,
        schema_name='registered_businesses'
    )

    with beam.Pipeline(options=pipeline_options) as p:
        lines = p | ReadFromText(known_args.input, coder=JsonCoder())

        load = (
                lines
                | beam.ParDo(ColumnsToLowerCase())
                | beam.ParDo(ParseAddress())
                | beam.ParDo(NormalizeAddress(StaticValueProvider(str, 'address_full')))
                | WriteToAvro(known_args.avro_output, schema=avro_schema, file_name_suffix='.avro', use_fastavro=True))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
