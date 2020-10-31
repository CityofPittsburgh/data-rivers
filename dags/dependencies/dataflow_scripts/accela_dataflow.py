from __future__ import absolute_import

import logging
import os

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.avroio import WriteToAvro

import dataflow_utils
from dataflow_utils.dataflow_utils import JsonCoder, SwapFieldNames, ColumnsCamelToSnakeCase, GeocodeAddress, \
    FilterFields, generate_args, get_schema, extract_field, extract_field_from_nested_list, geocode_address


class ParseNestedFields(beam.DoFn):

    def process(self, datum):

        if 'Traffic Obstruction' in datum['type']:

            extract_field_from_nested_list(datum, 'customForms', 0, 'Date (From)', 'from_date')
            extract_field_from_nested_list(datum, 'customForms', 0, 'Date (To)', 'to_date')
            datum['restoration_date'] = None

            try:
                datum['street_or_location'] = datum['customTables'][0]['rows'][0]['fields']['Street']
                datum['from_street'] = datum['customTables'][0]['rows'][0]['fields']['From']
                datum['to_street'] = datum['customTables'][0]['rows'][0]['fields']['To']
            except KeyError:
                datum['street_or_location'] = None
                datum['from_street'] = None
                datum['to_street'] = None

            extract_field_from_nested_list(datum, 'contacts', 0, 'organizationName', 'business_name')
            datum['license_type'] = None

        else:

            extract_field_from_nested_list(datum, 'customForms', 1, 'Valid Date (From)', 'from_date')
            extract_field_from_nested_list(datum, 'customForms', 1, 'Valid Date (To)', 'to_date')
            extract_field_from_nested_list(datum, 'customForms', 1, 'Restoration Date', 'restoration_date')
            extract_field_from_nested_list(datum, 'customForms', 1, 'Location', 'street_or_location')
            extract_field_from_nested_list(datum, 'customForms', 1, 'From', 'from_street')
            extract_field_from_nested_list(datum, 'customForms', 1, 'To', 'to_street')

            extract_field_from_nested_list(datum, 'professionals', 0, 'businessName', 'business_name')
            try:
                datum['license_type'] = datum['professionals'][0]['licenseType']['text']
            except KeyError:
                datum['license_type'] = None

        extract_field(datum, 'type', 'text', 'permit_type')
        # we want to call this single field 'type', but we need to get rid of the existing 'type' field
        # before we can do that
        datum.pop('type', None)
        datum['type'] = datum['permit_type']

        extract_field(datum, 'status', 'text', 'status_type')
        datum.pop('status', None)
        datum['status'] = datum['status_type']

        extract_field_from_nested_list(datum, 'addresses', 0, 'streetAddress', 'street_address')
        extract_field_from_nested_list(datum, 'addresses', 0, 'city', 'city')
        extract_field_from_nested_list(datum, 'addresses', 0, 'postalCode', 'postal_code')
        datum['address'] = F"{datum['street_address']} {datum['city']} PA {datum['postal_code']}"

        fields_to_remove = ['street_address',
                            'city',
                            'postal_code',
                            'addresses',
                            'professionals',
                            'customTables',
                            'customForms',
                            'contacts',
                            'permit_type',
                            'status_type']

        for field in fields_to_remove:
            datum.pop(field, None)

        yield datum


def run(argv=None):

    known_args, pipeline_options, avro_schema = generate_args(
        job_name='accela-permits',
        bucket='{}_accela'.format(os.environ['GCS_PREFIX']),
        argv=argv,
        schema_name='accela_permits'
    )

    with beam.Pipeline(options=pipeline_options) as p:

        exclude_fields = [
            'module',
            'serviceProviderCode',
            'undistributedCost',
            'totalJobCost',
            'recordClass',
            'reportedChannel',
            'closedByDepartment',
            'estimatedProductionUnit',
            'actualProductionUnit',
            'createdByCloning',
            'closedByUser',
            'trackingId',
            'initiatedProduct',
            'createdBy',
            'value',
            'balance',
            'booking',
            'infraction',
            'misdemeanor',
            'offenseWitnessed',
            'defendantSignature',
            'parcels',
            'id',
            'statusDate',
            'jobValue',
            'reportedDate'
        ]

        address_field = 'address'

        field_name_swaps = [
            ('customId', 'id'),
            ('totalPay', 'total_paid')
        ]

        lines = p | ReadFromText(known_args.input, coder=JsonCoder())

        load = (
                lines
                | beam.ParDo(FilterFields(exclude_fields))
                | beam.ParDo(ParseNestedFields())
                | beam.ParDo(GeocodeAddress(address_field))
                | beam.ParDo(SwapFieldNames(field_name_swaps))
                | beam.ParDo(ColumnsCamelToSnakeCase())
                | WriteToAvro(known_args.avro_output, schema=avro_schema, file_name_suffix='.avro', use_fastavro=True))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
