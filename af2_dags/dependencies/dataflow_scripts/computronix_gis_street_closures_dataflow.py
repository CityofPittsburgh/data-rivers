from __future__ import absolute_import

import logging
import os

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.avroio import WriteToAvro

# import util modules.
# util modules located one level down in directory (./dataflow_util_modules/datflow_utils.py)
from dataflow_utils.dataflow_utils import JsonCoder, SwapFieldNames, ConvertBooleans, StandardizeTimes, \
    FilterFields, ConvertStringCase, generate_args


# The CX data contains fields that are nested. We need to extract that information, which is accomplished by this
# function. Additionally, the new field names that we derive are formatted in snake case. Several IDs for segments of
# street closures are convert to strings explicity. The CX system returns some IDs as strings and others as INTs,
# so they are standarized here.

# Sometimes many rows created from one. If there is more than one segment of street closed, a separate row is
# created for eacah. For each of the multiple segments All values, except the segment IDs are identical and redundant
# (all fields other than the IDs). Though this is inefficient from a data storage perspective, it is needed by the
# GIS team for compliance with their software as of 05/2022


class UnNestRenameFields(beam.DoFn):
    def __init__(self, nested_data):
        """

        """
        self.nested_data = nested_data

    def process(self, datum):
        old_nest_keys = self.nested_data[0]
        new_unnest_keys = self.nested_data[1]

        # if there are values to unnest
        if datum["street_closure"]:
            # iterate through all nested fields and extract them
            for (onk, nuk) in zip(old_nest_keys, new_unnest_keys):
                datum[nuk] = datum["street_closure"][0][onk]

            # there can be multiple segments per ticket; each segment needs to be made a separate row,
            # and all information needs to be present in each row, with the only difference being the segment. Thus,
            # two segments from the same record will have redundant information, with only the segment information
            # being unique. this is easy to accomplish by first extracting the segments to a local var, then looping
            # over them, each time simply adding the segment info to the existing copy of datum (preserving the data)
            segs = datum["street_segment"]
            # total_num = len(segs)
            # loop through the segments
            # for s, num in (segs, range(total_num)):
            for s in segs:

                datum["closure_id"] = str(s["UNIQUEID"])
                datum["carte_id"] = str(s["CARTEID"])

                # yield will return datum without exiting the loop
                yield datum

        # if there is not a street segment, the relevant columns don't exist. all columns must be present in each datum,
        # so we populate them with None values here
        else:
            for (onk, nuk) in zip(old_nest_keys, new_unnest_keys):
                datum[nuk] = None
            datum["closure_id"] = None
            datum["carte_id"] = None

            yield datum


def run(argv = None):
    known_args, pipeline_options, avro_schema = generate_args(
            job_name = 'computronix-domi-street-closures-gis',
            bucket = '{}_computronix'.format(os.environ['GCS_PREFIX']),
            argv = argv,
            schema_name = 'computronix_gis_street_closures'
    )

    with beam.Pipeline(options = pipeline_options) as p:
        name_swaps = [("EXTERNALFILENUM", "ext_file_num"), ("PERMITTYPEPERMITTYPE", "permit_type"),
                      ("WORKDESCRIPTION", "work_desc"), ("TYPEOFWORKDESCRIPTION", "type_work_desc"),
                      ("APPLICANTCUSTOMFORMATTEDNAME", "applicant_name"),
                      ("ALLCONTRACTORSNAME", "contractor_name"), ("CREATEDDATE", "create_date"),
                      ("DOMISTREETCLOSURE", "street_closure")
                      ]

        un_nest_names = ["SPECIALINSTRUCTIONS", "FROMDATE", "TODATE", "WEEKDAYWORKHOURS", "WEEKENDWORKHOURS",
                         "PRIMARYSTREET", "FROMSTREET", "TOSTREET", "FULLCLOSURE", "TRAVELLANE", "PARKINGLANE",
                         "METEREDPARKING", "SIDEWALK", "VALIDATED", "STREETCLOSUREDOMISTREETSEGMENT"]
        new_names = ["special_instructions", "from_date", "to_date", "weekday_hours", "weekend_hours", "primary_street",
                     "from_street", "to_street", "full_closure", "travel_lane", "parking_lane", "metered_parking",
                     "sidewalk", "validated", "street_segment"]
        nested = [un_nest_names, new_names]

        times = [("create_date", "EST"), ("from_date", "EST"), ("to_date", "EST")]

        bool_convs = [("full_closure", "Y", "N", False), ("travel_lane", "Y", "N", False),
                      ("parking_lane", "Y", "N", False), ("metered_parking", "Y", "N", False),
                      ("sidewalk", "Y", "N", False), ("validated", "Y", "N", False)]

        str_convs = [("ext_file_num", "upper"), ("permit_type", "title"), ("work_desc", "sentence"),
                     ("type_work_desc", "title"), ("contractor_name", "title"), ("special_instructions", "sentence"),
                     ("weekday_hours", "upper"), ("weekend_hours", "upper"), ("primary_street", "upper"),
                     ("from_street", "upper"), ("to_street", "upper")]

        drops = ["create_date", "from_date", "to_date", "street_segment", "street_closure"]

        lines = p | ReadFromText(known_args.input, coder = JsonCoder())

        load = (
                lines
                | beam.ParDo(SwapFieldNames(name_swaps))
                | beam.ParDo(UnNestRenameFields(nested))
                | beam.ParDo(ConvertBooleans(bool_convs, True))
                | beam.ParDo(ConvertStringCase(str_convs))
                | beam.ParDo(StandardizeTimes(times))
                | beam.ParDo(FilterFields(drops))
                | WriteToAvro(known_args.avro_output, schema = avro_schema, file_name_suffix = '.avro',
                              use_fastavro = True))

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
