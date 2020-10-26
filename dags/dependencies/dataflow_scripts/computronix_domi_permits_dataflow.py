from __future__ import absolute_import

import logging
import os
import requests

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.avroio import WriteToAvro

import dataflow_utils
from dataflow_utils.dataflow_utils import JsonCoder, SwapFieldNames, generate_args, get_schema, geocode_address


class ParseNestedFields(beam.DoFn):

    def extract_field(self, datum, source_field, nested_field, new_field_name):
        """
        :param datum: datum in PCollection
        :param source_field: name of field containing nested values
        :param nested_field: name of field nested within source_field dict the value of which we want to extract
        and assign its value to new_field_name
        :param new_field_name: name for new field we're creating with the value of nested_field
        :return: datum in PCollection
        """
        if datum[source_field] and nested_field in datum[source_field]:
            datum[new_field_name] = datum[source_field][nested_field]
        else:
            datum[new_field_name] = None

    def process(self, datum):
        self.extract_field(datum, 'ADDRESS', 'LATITUDE', 'lat')
        self.extract_field(datum, 'ADDRESS', 'LONGITUDE', 'long')
        self.extract_field(datum, 'ADDRESS', 'FORMATTEDADDRESS', 'ADDRESSFULL')
        del datum['ADDRESS']
        # all ADDRESS vals have a '-' at the end; also ADDRESS is a more sensible field name than ADDRESSFULL,
        # but we need to delete the nested ADDRESS dict before we can add a new string field called ADDRESS
        if datum['ADDRESSFULL']:
            datum['ADDRESS'] = datum['ADDRESSFULL'][:-1]
        else:
            datum['ADDRESS'] = None
        del datum['ADDRESSFULL']

        self.extract_field(datum, 'DOMISTREETCLOSURE', 'FROMDATE', 'STREETCLOSUREFROMDATE')
        self.extract_field(datum, 'DOMISTREETCLOSURE', 'TODATE', 'STREETCLOSURETODATE')
        self.extract_field(datum, 'DOMISTREETCLOSURE', 'OBJECTID', 'STREETCLOSUREID')
        self.extract_field(datum, 'DOMISTREETCLOSURE', 'FROMDATE', 'STREETCLOSUREFROMDATE')
        self.extract_field(datum, 'DOMISTREETCLOSURE', 'WEEKDAYWORKHOURS', 'WEEKDAYWORKHOURS')
        self.extract_field(datum, 'DOMISTREETCLOSURE', 'WEEKENDWORKHOURS', 'WEEKENDWORKHOURS')
        self.extract_field(datum, 'DOMISTREETCLOSURE', 'FULLCLOSURE', 'FULLSTREETCLOSURE')
        self.extract_field(datum, 'DOMISTREETCLOSURE', 'SIDEWALK', 'SIDEWALKCLOSED')
        del datum['DOMISTREETCLOSURE']

        self.extract_field(datum, 'LOCATION', 'DESCRIPTION', 'LOCATIONDESCRIPTION')
        self.extract_field(datum, 'LOCATION', 'FROMCROSSSTREET', 'LOCATIONCROSSSTREETFROM')
        self.extract_field(datum, 'LOCATION', 'TOCROSSSTREET', 'LOCATIONCROSSSTREETTO')
        del datum['LOCATION']

        yield datum


class GeocodeAddress(beam.DoFn):
    """
    Geocode if datum has address but no lat/long
    """
    def process(self, datum):

        if datum['ADDRESS'] and (not datum['lat'] or not datum['long']):
            coords = geocode_address(datum['ADDRESS'])
            datum['lat'] = coords['lat']
            datum['long'] = coords['long']
        else:
            pass

        yield datum


def run(argv=None):

    known_args, pipeline_options, avro_schema = generate_args(
        job_name='computronix-domi-permits',
        bucket='{}_computronix'.format(os.environ['GCS_PREFIX']),
        argv=argv,
        schema_name='domi_permits_computronix'
    )

    with beam.Pipeline(options=pipeline_options) as p:

        field_name_swaps = [
            ('PERMITTYPEPERMITTYPE', 'PERMITTYPE'),
            ('TYPEOFWORKDESCRIPTION', 'WORKTYPE'),
            ('APPLICANTCUSTOMFORMATTEDNAME', 'APPLICANTNAME'),
            ('ALLCONTRACTORSNAME', 'CONTRACTORNAME'),
            ('SPECIALPERMITINSTRUCTIONS', 'SPECIALINSTRUCTIONS'),
            ('STATUSDESCRIPTION', 'STATUS')
        ]

        lines = p | ReadFromText(known_args.input, coder=JsonCoder())

        load = (
                lines
                | beam.ParDo(ParseNestedFields())
                | beam.ParDo(SwapFieldNames(field_name_swaps))
                | beam.ParDo(GeocodeAddress())
                | WriteToAvro(known_args.avro_output, schema=avro_schema, file_name_suffix='.avro', use_fastavro=True))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
