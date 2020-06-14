from __future__ import absolute_import

import argparse
import logging
import os

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.avroio import WriteToAvro
from apache_beam.options.pipeline_options import PipelineOptions

from dataflow_utils import dataflow_utils
from dataflow_utils.dataflow_utils import generate_args, get_schema, JsonCoder


class GetStatus(beam.DoFn):
    def process(self, datum):
        status = ''
        if datum['status'] == 0:
            status = 'open'
        elif datum['status'] == 1:
            status = 'closed'
        elif datum['status'] == 3:
            status == 'in progress'
        elif datum['status'] == 4:
            status = 'on hold'
        else:
            pass
        datum['status'] = status
        yield datum


class CleanLatLong(beam.DoFn):
    def process(self, datum):
        datum['lat'] = datum['latitude']
        datum['long'] = datum['longitude']
        datum.pop('latitude')
        datum.pop('longitude')
        yield datum


def run(argv=None):
    parser = argparse.ArgumentParser()
    #
    # parser.add_argument('--input',
    #                     dest='input',
    #                     default='gs://{}_qalert/requests/{}/{}/{}_requests.json'.format(os.environ['GCS_PREFIX'],
    #                                                                                             dt.strftime('%Y'),
    #                                                                                             dt.strftime('%m').lower(),
    #                                                                                             dt.strftime("%Y-%m-%d")),
    #                     help='Input file to process.')
    # parser.add_argument('--avro_output',
    #                     dest='avro_output',
    #                     default='gs://{}_qalert/requests/avro_output/{}/{}/{}/avro_output'.format(os.environ['GCS_PREFIX'],
    #                                                                                          dt.strftime('%Y'),
    #                                                                                          dt.strftime('%m').lower(),
    #                                                                                          dt.strftime("%Y-%m-%d")),
    #                     help='Output directory to write avro files.')

    known_args, pipeline_args = parser.parse_known_args(argv)

    #TODO: run on on-prem network when route is opened
    # Use runner=DataflowRunner to run in GCP environment, DirectRunner to run locally
    pipeline_args.extend(generate_args('qalert-requests-dataflow',
                                       '{}_qalert'.format(os.environ['GCS_PREFIX']),
                                       'DataflowRunner'))

    pipeline_args.append('--setup_file={}'.format(os.environ['SETUP_PY_DATAFLOW']))

    avro_schema = get_schema('City_of_Pittsburgh_QAlert_Requests')

    pipeline_options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=pipeline_options) as p:
        # Read the text file[pattern] into a PCollection.
        lines = p | ReadFromText(known_args.input, coder=JsonCoder())

        load = (
                lines
                | beam.ParDo(GetStatus())
                | beam.ParDo(CleanLatLong())
                | WriteToAvro(known_args.avro_output, schema=avro_schema, file_name_suffix='.avro', use_fastavro=True))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()