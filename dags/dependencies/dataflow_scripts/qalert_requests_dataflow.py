from __future__ import absolute_import

import logging
import os

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.avroio import WriteToAvro

from dataflow_utils import dataflow_utils
from dataflow_utils.dataflow_utils import JsonCoder, SwapFieldNames, generate_args, \
    FilterFields, ColumnsCamelToSnakeCase, GetDateStringsFromUnix,ChangeDataTypes, \
    unix_to_date_strings, GoogleMapsClassifyAndGeocode


class GetStatus(beam.DoFn):
    def process(self, datum):
        status_name = ''
        if datum['status_code'] == "0":
            datum["status_name"] = 'open'
        elif datum['status_code'] == "1":
            datum["status_name"] = 'closed'
        elif datum['status_code'] == "3":
            datum["status_name"] = 'in progress'
        elif datum['status_code'] == "4":
            datum["status_name"] = 'on hold'
        else:
            pass
        datum['status_name'] = status_name
        yield datum


class GetClosedDate(beam.DoFn):
    def process(self, datum):
        if datum['status_name'] == 'closed':
            datum['closed_date_unix'] = datum['last_action_unix']
            datum['closed_date_utc'], datum['closed_date_est'] = unix_to_date_strings(datum['last_action_unix'])
        else:
            datum['closed_date_unix'], datum['closed_date_utc'], datum['closed_date_est'] = None, None, None
        yield datum


class DetectChildTicketStatus(beam.DoFn):
    def process(self, datum):
        if datum['parent_ticket'] == "0":
            datum['child_ticket'] = False
        else:
            datum['child_ticket'] = True
        yield datum


def run(argv = None):
    # assign the name for the job and specify the AVRO upload location (GCS bucket), arg parser object,
    # and avro schema to validate data with. Return the arg parser values, PipelineOptions, and avro_schemas (dict)

    known_args, pipeline_options, avro_schema = generate_args(
            job_name = 'qalert-requests-dataflow',
            bucket = '{}_qalert'.format(os.environ['GCS_PREFIX']),
            argv = argv,
            schema_name = 'City_of_Pittsburgh_QAlert_Requests'
    )

    with beam.Pipeline(options = pipeline_options) as p:
        field_name_swaps = [('master', 'parent_ticket'),
                            ('addDateUnix', 'create_date_unix'),
                            ('lastActionUnix', 'last_action_unix'),
                            ("status", "status_code"),
                            ('streetName', 'street'),
                            ("crossStreetName", "cross_street"),
                            ("comments", "pii_comments"),
                            ("privateNotes", "pii_private_notes"),
                            ("latitude", "lat"),
                            ("longitude", "long"),
                            ("cityName", "city")]

        drop_fields = ['addDate', 'lastAction', 'displayDate', 'displayLastAction',
                       'district', 'submitter', 'priorityValue', 'aggregatorID',
                       'priorityToDisplay', 'aggregatorInfo']

        date_conversions = [('last_action_unix', 'last_action_utc', 'last_action_est'),
                            ('create_date_unix', 'create_date_utc', 'create_date_est')]

        type_changes = [("id", "str"), ("parent_ticket", "str"), ("status_code", "str"), ("street_id", "str"),
                        ("type_id", "str")]



        lines = p | ReadFromText(known_args.input, coder = JsonCoder())

        load = (
                lines
                | beam.ParDo(SwapFieldNames(field_name_swaps))
                | beam.ParDo(FilterFields(drop_fields))
                | beam.ParDo(ColumnsCamelToSnakeCase())
                | beam.ParDo(GetDateStringsFromUnix(date_conversions))
                | beam.ParDo(ChangeDataTypes(type_changes))
                | beam.ParDo(GetStatus())
                | beam.ParDo(GetClosedDate())
                | beam.ParDo(DetectChildTicketStatus())
                # | beam.ParDo(GoogleMapsClassifyAndGeocode(loc_field_names))

                # TODO: change the schema after it is created
                | WriteToAvro(known_args.avro_output, schema = avro_schema, file_name_suffix = '.avro', use_fastavro=True)
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
