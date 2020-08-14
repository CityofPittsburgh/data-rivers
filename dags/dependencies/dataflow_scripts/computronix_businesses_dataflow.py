from __future__ import absolute_import

import argparse
import logging
import os

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.avroio import WriteToAvro

from dataflow_utils import dataflow_utils
from dataflow_utils.dataflow_utils import generate_args, get_schema, JsonCoder


#TODO: add step to normalize address, add normalized_address to .avsc

class FormatColumnNames(beam.DoFn):
    def process(self, datum):
        cleaned_col_names = {
            'LICENSENUMBER': u'license_number',
            'LICENSETYPENAME': u'license_type_name',
            'NAICSCODE': u'naics_code',
            'BUSINESSNAME': u'business_name',
            'LICENSESTATE': u'license_state',
            'INITIALISSUEDATE': u'initial_issue_date',
            'MOSTRECENTISSUEDATE': u'most_recent_issue_date',
            'EFFECTIVEDATE': u'effective_date',
            'EXPIRATIONDATE': u'expiration_date',
            'INSURANCEEXPIRATIONDATE': u'insurance_expiration_date',
            'NUMBEROFEMPLOYEES': u'number_of_employees',
            'NUMBEROFSIGNSTOTAL': u'number_of_signs_total',
            'NUMBEROFSMALLSIGNS': u'number_of_small_signs',
            'NUMBEROFLARGESIGNS': u'number_of_large_signs',
            'TOTALNUMBEROFSPACES': u'total_number_of_spaces',
            'NUMBEROFNONLEASEDPUBSPACES': u'number_of_nonleased_pub_spaces',
            'NUMBEROFREVGENSPACES': u'number_of_revgen_spaces',
            'NUMBEROFHANDICAPSPACES': u'number_of_handicap_spaces',
            'NUMBEROFSEATS': u'number_of_seats',
            'NUMBEROFNONGAMBLINGMACHINES': u'number_of_nongambling_machines',
            'NUMBEROFPOOLTABLES': u'number_of_pool_tables',
            'NUMBEROFJUKEBOXES': u'number_of_jukeboxes',
            'PARCELNUMBER': u'parcel_number',
            'ADDRESSFORMATTEDADDRESS': u'address',
        }
        formatted = dict((cleaned_col_names[key], value) for (key, value) in datum.items())
        # some of these items don't have all fields included
        for k, v in cleaned_col_names.items():
            if v not in formatted:
                formatted[v] = None
        yield formatted


class ConvertTypes(beam.DoFn):
    def process(self, datum):
        datum['naics_code'] = int(datum['naics_code'])
        yield datum


def run(argv=None):
    known_args, pipeline_options, avro_schema = generate_args(
        job_name='computronix-businesses-dataflow',
        bucket='{}_computronix'.format(os.environ['GCS_PREFIX']),
        argv=argv,
        schema_name='businesses_computronix',
        runner='DataflowRunner'
    )

    with beam.Pipeline(options=pipeline_options) as p:
        # Read the text file[pattern] into a PCollection.
        lines = p | ReadFromText(known_args.input, coder=JsonCoder())

        load = (
                lines
                | beam.ParDo(FormatColumnNames())
                | beam.ParDo(ConvertTypes())
                | WriteToAvro(known_args.avro_output, schema=avro_schema, file_name_suffix='.avro', use_fastavro=True))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
