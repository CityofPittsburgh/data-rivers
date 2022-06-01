from __future__ import absolute_import

import logging
import os

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.avroio import WriteToAvro


# import util modules.
# util modules located one level down in directory (./dataflow_util_modules/datflow_utils.py)
from dataflow_utils.dataflow_utils import JsonCoder, SwapFieldNames, \
    generate_args, FilterFields, ColumnsCamelToSnakeCase, GetDateStringsFromUnix, ChangeDataTypes, \
    unix_to_date_strings, GoogleMapsClassifyAndGeocode, AnonymizeAddressBlock, AnonymizeLatLong


class GetStatus(beam.DoFn):
    def process(self, datum):
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
        yield datum


class GetClosedDate(beam.DoFn):
    def process(self, datum):
        if datum['status_name'] == 'closed':
            datum['closed_date_unix'] = datum['last_action_unix']
            datum['closed_date_utc'], datum['closed_date_est'] = unix_to_date_strings(datum['last_action_unix'])
        else:
            datum['closed_date_unix'], datum['closed_date_utc'], datum['closed_date_est'] = None, None, None
        datum.pop('close_date')
        yield datum


class DetectChildTicketStatus(beam.DoFn):
    def process(self, datum):
        if datum['parent_ticket_id'] == "0":
            datum['child_ticket'] = False
        else:
            datum['child_ticket'] = True
        yield datum


def run(argv = None):
    # assign the name for the job and specify the AVRO upload location (GCS bucket), arg parser object,
    # and avro schema to validate data with. Return the arg parser values, PipelineOptions, and avro_schemas (dict)

    known_args, pipeline_options, avro_schema = generate_args(
            job_name = 'qalert-requests-dataflow',
            bucket = f"{os.environ['GCS_PREFIX']}_qalert",
            argv = argv,
            schema_name = 'qalert_requests',
            limit_workers = [False, None]
    )

    with beam.Pipeline(options = pipeline_options) as p:
        field_name_swaps = [('master', 'parent_ticket_id'),
                            ('addDateUnix', 'create_date_unix'),
                            ('lastActionUnix', 'last_action_unix'),
                            ("status", "status_code"),
                            ('streetNum', 'pii_street_num'),
                            ('streetName', 'street'),
                            ("crossStreetName", "cross_street"),
                            ("comments", "pii_comments"),
                            ("privateNotes", "pii_private_notes"),
                            ("latitude", "pii_lat"),
                            ("longitude", "pii_long"),
                            ("cityName", "city"),
                            ('typeId', 'request_type_id'),
                            ('typeName', 'request_type_name')]

        drop_fields = ['addDate', 'lastAction', 'displayDate', 'displayLastAction',
                       'district', 'submitter', 'priorityValue', 'aggregatorId',
                       'priorityToDisplay', 'aggregatorInfo', 'resumeDate', "cityId"]

        date_conversions = [('last_action_unix', 'last_action_utc', 'last_action_est'),
                            ('create_date_unix', 'create_date_utc', 'create_date_est')]

        type_changes = [("id", "str"), ("parent_ticket_id", "str"), ("status_code", "str"), ("street_id", "str"),
                        ("cross_street_id", "str"), ("request_type_id", "str")]

        gmap_key = os.environ["GMAP_API_KEY"]

        loc_names = {
                "street_num_field"  : "pii_street_num",
                "street_name_field" : "street",
                "cross_street_field": "cross_street",
                "city_field"        : "city",
                "lat_field"         : "pii_lat",
                "long_field"        : "pii_long"
        }

        block_anon_accuracy = [("pii_google_formatted_address", 100)]
        lat_long_accuracy = [("pii_lat", "pii_long", 200)]

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
                | beam.ParDo(GoogleMapsClassifyAndGeocode(key = gmap_key, loc_field_names = loc_names,
                                                          partitioned_address = True, contains_pii = True,
                                                          del_org_input = False))
                | beam.ParDo(AnonymizeLatLong(lat_long_accuracy))
                | beam.ParDo(AnonymizeAddressBlock(block_anon_accuracy))
                | WriteToAvro(known_args.avro_output, schema = avro_schema, file_name_suffix = '.avro',
                              use_fastavro = True)
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
