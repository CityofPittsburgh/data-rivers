from __future__ import absolute_import

import logging
import os

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.avroio import WriteToAvro

from dataflow_utils import dataflow_utils
from dataflow_utils.dataflow_utils import JsonCoder, SwapFieldNames, GetDateStrings, generate_args, get_schema


class GetStatus(beam.DoFn):
    def process(self, datum):
        status = ''
        if datum['statusCode'] == 0:
            status = 'open'
        elif datum['statusCode'] == 1:
            status = 'closed'
        elif datum['statusCode'] == 3:
            status = 'in progress'
        elif datum['statusCode'] == 4:
            status = 'on hold'
        else:
            pass
        datum['status'] = status
        yield datum


class GetClosedDate(beam.DoFn):
    def process(self, datum):
        if datum['status'] == 'closed':
            datum['closedOn'] = datum['lastAction']
            datum['closedOnUnix'] = datum['lastActionUnix']
        else:
            datum['closedOn'] = None
            datum['closedOnUnix'] = None
        yield datum


def run(argv=None):
    """
    If you want to run just this file for rapid development, add the arg '-r DirectRunner' and add
    GCS paths for --input and --avro_output, e.g.
    python qalert_requests_dataflow.py --input gs://pghpa_test_qalert/requests/2020/06/2020-06-17_requests.json
    --avro_output gs://pghpa_test_qalert/requests/avro_output/2020/06/2020-06-17/ -r DirectRunner
    """

    known_args, pipeline_options, avro_schema = generate_args(
        job_name='qalert-requests-dataflow',
        bucket='{}_qalert'.format(os.environ['GCS_PREFIX']),
        argv=argv,
        schema_name='City_of_Pittsburgh_QAlert_Requests'
    )

    with beam.Pipeline(options=pipeline_options) as p:

        date_conversions = [('lastActionUnix', 'lastAction'), ('addDateUnix', 'createDate')]
        field_name_swaps = [('addDateUnix', 'createDateUnix'),
                            ('status', 'statusCode'),
                            ('latitude', 'lat'),
                            ('longitude', 'long'),
                            ('master', 'masterRequestId'),
                            ('typeId', 'requestTypeId'),
                            ('typeName', 'requestType')]

        lines = p | ReadFromText(known_args.input, coder=JsonCoder())

        load = (
                lines
                | beam.ParDo(GetDateStrings(date_conversions))
                | beam.ParDo(SwapFieldNames(field_name_swaps))
                | beam.ParDo(GetStatus())
                | beam.ParDo(GetClosedDate())
                | WriteToAvro(known_args.avro_output, schema=avro_schema, file_name_suffix='.avro', use_fastavro=True))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
