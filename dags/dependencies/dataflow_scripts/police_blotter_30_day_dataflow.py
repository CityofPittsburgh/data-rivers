from __future__ import absolute_import

import os
import logging

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.avroio import WriteToAvro

from dataflow_utils.dataflow_utils import generate_args, ChangeDataTypes, JsonCoder, SwapFieldNames


class CleanPKs(beam.DoFn):
    """
    For whatever weird reason, some IDs in this data are negative numbers, so we make them all positive in this step
    """
    def process(self, datum):
        datum['PK'] = abs(datum['PK'])
        yield datum


def run(argv=None):

    known_args, pipeline_options, avro_schema = generate_args(
        job_name='police-blotter-30-day-dataflow',
        bucket='{}_police'.format(os.environ['GCS_PREFIX']),
        argv=argv,
        schema_name='30_day_police_blotter'
    )

    with beam.Pipeline(options=pipeline_options) as p:
        lines = p | ReadFromText(known_args.input, coder=JsonCoder())

        data_type_changes = [('CCR', 'int'), ('TRACT', 'int')]
        field_name_swaps = [('PK', 'id')]

        load = (
                lines
                | beam.ParDo(CleanPKs())
                | beam.ParDo(ChangeDataTypes(data_type_changes))
                | beam.ParDo(SwapFieldNames(field_name_swaps))
                | WriteToAvro(known_args.avro_output, schema=avro_schema, file_name_suffix='.avro', use_fastavro=True))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
