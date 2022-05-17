from __future__ import absolute_import

import logging
import os

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.avroio import WriteToAvro

import dataflow_utils
from dataflow_utils.dataflow_utils import JsonCoder, SwapFieldNames, ChangeDataTypes, ConvertBooleans, \
    FilterOutliers, StandardizeTimes, GoogleMapsClassifyAndGeocode, generate_args


# The CX data contains fields that are nested. We need to extract that information, which is accomplished by this
# function. Additionally, the new field names that we derive are formatted in snake case.
class UnNestFields(beam.DoFn, ABC):
    def __init__(self, un_nest):
        """
        :param un_nest: list of tuples; each tuple consists of the 1) the parent field we want to un_nest from
        2) a list of the new field names for the unnested child fields
        """
        self.un_nest = un_nest


# TODO: what needs to happen here is we need to unnest all fields and give them a new new. however,
# the problem is that there are sometimes going to be many rows created from 1. if there is more than one carte id
# you have to make that many rows
# so pass in the field (parent and child)
# then loop thru each val and each time create a new row with copied values redundant and the new stuff in carte id etc
# to do this- create a new var ("datum 2") and return that. this will basically take the place of datum original
# don't have to return input datum it's not useful;
# carte id is the only field we care about for determing new rows


def run(argv = None):
    known_args, pipeline_options, avro_schema = generate_args(
            job_name = 'computronix-domi-street-closures-gis',
            bucket = '{}_computronix'.format(os.environ['GCS_PREFIX']),
            argv = argv,
            schema_name = 'domi_permits_computronix_2022_refactor'
    )

    with beam.Pipeline(options = pipeline_options) as p:
        un_nest_fields = [
                ["SPECIALINSTRUCTIONS", "FROMDATE", "TODATE", "WEEKDAYWORKHOURS", "WEEKENDWORKHOURS", "PRIMARYSTREET",
                 "FROMSTREET", "TOSTREET", "FULLCLOSURE", "TRAVELLANE", "PARKINGLANE", "METEREDPARKING", "SIDEWALK",
                 "VALIDATED"],
                ["UNIQUEID", "CARTEID"]
        ]

        new_names = [
                ["special_instructions", "from_date", "to_date", "weekday_hours", "weekend_hours", "primary_street",
                 "from_street", "to_street", "full_closure", "travel_lane", "parking_lane", "metered_parking",
                 "sidewalk", "validated"],
                ["unique_id", "carte_id"]
        ]

        un_nest_data = [
                ('DOMISTREETCLOSURE', un_nest_fields[0], new_names[0]),
                ("STREETCLOSUREDOMISTREETSEGMENT", un_nest_fields[1], new_names[1])
        ]

        lines = p | ReadFromText(known_args.input, coder = JsonCoder())

        load = (
                lines
                | beam.ParDo(UnNestFields(un_nest_data))
                # | beam.ParDo(SwapFieldNames(field_name_swaps))
                | WriteToAvro(known_args.avro_output, schema = avro_schema, file_name_suffix = '.avro',
                              use_fastavro = True))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
