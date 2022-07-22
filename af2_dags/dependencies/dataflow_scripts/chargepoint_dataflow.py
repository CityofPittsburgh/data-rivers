from __future__ import absolute_import

import logging
import os

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.avroio import WriteToAvro

from dataflow_utils import dataflow_utils
from dataflow_utils.dataflow_utils import JsonCoder, SwapFieldNames, generate_args, FilterFields, \
    ColumnsCamelToSnakeCase, GetDateStringsFromUnix, ChangeDataTypes, StandardizeTimes, \
    GoogleMapsClassifyAndGeocode, AnonymizeAddressBlock, AnonymizeLatLong


def run(argv = None):
    # assign the name for the job and specify the AVRO upload location (GCS bucket), arg parser object,
    # and avro schema to validate data with. Return the arg parser values, PipelineOptions, and avro_schemas (dict)

    known_args, pipeline_options, avro_schema = generate_args(
            job_name = 'chargepoint-dataflow',
            bucket = f"{os.environ['GCS_PREFIX']}_chargepoint",
            argv = argv,
            schema_name = 'chargepoint_sessions',
            limit_workers = [False, None]
    )

    with beam.Pipeline(options = pipeline_options) as p:
        field_name_swaps = [('postalCode', 'zip'), ('Energy', 'energy_kwh')]
        times = [('start_time', 'UTC'), ('end_time', 'UTC')]
        type_changes = [('port_number', 'str'), ('session_id', 'str'),
                        ('zip', 'str'), ('energy_kwh', 'float')]
        drop_fields = ['recordNumber', 'start_time', 'end_time']

        lines = p | ReadFromText(known_args.input, coder = JsonCoder())

        load = (
                lines
                | beam.ParDo(SwapFieldNames(field_name_swaps))
                | beam.ParDo(ColumnsCamelToSnakeCase())
                | beam.ParDo(StandardizeTimes(times))
                | beam.ParDo(ChangeDataTypes(type_changes))
                | beam.ParDo(FilterFields(drop_fields))
                | WriteToAvro(known_args.avro_output, schema = avro_schema, file_name_suffix = '.avro',
                              use_fastavro = True)
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
