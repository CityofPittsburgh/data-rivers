from __future__ import absolute_import

import logging
import os

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.avroio import WriteToAvro

import dataflow_utils
from dataflow_utils.dataflow_utils import JsonCoder, SwapFieldNames, ChangeDataTypes, ConvertBooleans, \
    FilterOutliers, StandardizeTimes, GoogleMapsClassifyAndGeocode, generate_args

MIN_DUMPSTERS = 0
MAX_DUMPSTERS = 20
MIN_MACHINES = 0
MAX_MACHINES = 20


# The CX data contains fields that are nested. We need to extract that information, which is accomplished by this
# function. Additionally, the new field names that we derive are formatted in snake case.
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
            datum['address'] = datum['ADDRESSFULL'][:-1]
        else:
            datum['address'] = None
        del datum['ADDRESSFULL']

        # id
        self.extract_field(datum, 'DOMISTREETCLOSURE', 'JOBID', 'closure_job_id')
        self.extract_field(datum, 'DOMISTREETCLOSURE', 'OBJECTID', 'street_close_obj_id')
        # time points
        self.extract_field(datum, 'DOMISTREETCLOSURE', 'FROMDATE', 'street_close_date_start')
        self.extract_field(datum, 'DOMISTREETCLOSURE', 'TODATE', 'street_close_stop')
        self.extract_field(datum, 'DOMISTREETCLOSURE', 'WEEKDAYWORKHOURS', 'street_close_wkday_hours')
        self.extract_field(datum, 'DOMISTREETCLOSURE', 'WEEKENDWORKHOURS', 'street_close_wkend_hours')
        # location
        self.extract_field(datum, 'DOMISTREETCLOSURE', 'PRIMARYSTREET', 'primary_street')
        self.extract_field(datum, 'DOMISTREETCLOSURE', 'FROMSTREET', 'closure_origin')
        self.extract_field(datum, 'DOMISTREETCLOSURE', 'TOSTREET', 'closure_end_pt')
        # bool_indicators
        self.extract_field(datum, 'DOMISTREETCLOSURE', 'CALCULATECLOSURE', 'calc_closure')
        self.extract_field(datum, 'DOMISTREETCLOSURE', 'METEREDPARKING', 'metered_parking')
        self.extract_field(datum, 'DOMISTREETCLOSURE', 'PARKINGLANE', 'parking_lane')
        self.extract_field(datum, 'DOMISTREETCLOSURE', 'SIDEWALK', 'sidewalk')
        self.extract_field(datum, 'DOMISTREETCLOSURE', 'PRINTONPERMIT', 'print_on_permit')
        self.extract_field(datum, 'DOMISTREETCLOSURE', 'TRAVELLANE', 'travel_lane')
        self.extract_field(datum, 'DOMISTREETCLOSURE', 'VALIDATED', 'validated')
        self.extract_field(datum, 'DOMISTREETCLOSURE', 'FULLCLOSURE', 'full_st_close')

        del datum['DOMISTREETCLOSURE']

        self.extract_field(datum, 'LOCATION', 'DESCRIPTION', 'loc_desc')
        self.extract_field(datum, 'LOCATION', 'FROMCROSSSTREET', 'cross_st_origin')
        self.extract_field(datum, 'LOCATION', 'TOCROSSSTREET', 'cross_st_end_pt')
        del datum['LOCATION']

        yield datum


def run(argv = None):
    known_args, pipeline_options, avro_schema = generate_args(
            job_name = 'computronix-domi-permits',
            bucket = '{}_computronix'.format(os.environ['GCS_PREFIX']),
            argv = argv,
            schema_name = 'domi_permits_computronix_2022_refactor'
    )

    with beam.Pipeline(options = pipeline_options) as p:
        # the CX data is sometimes nested, which is unnested by the helper function declared above. Within that
        # function new fields are created and the naming convention that is used is snake case. Thus, some of the new
        # field names are derived from that parsing function and the rest are changed here.
        field_name_swaps = [("STATUSDESCRIPTION", "status"),
                            ("APPLICANTCUSTOMFORMATTEDNAME", "applicant_formatted_name"),
                            ("ALLCONTRACTORSNAME", "contractors_name"),
                            ("APPLICANTCUSTOMEROBJECTID", "applicant_obj_id"),
                            ("JOBID", "id"),
                            ("PARENTJOBID", "parent_id"),
                            ("EXTERNALFILENUM", "ext_file_num"),
                            ("WORKDESCRIPTION", "work_desc"),
                            ("TYPEOFWORKDESCRIPTION", "type_work"),
                            ("COMMERCIALORRESIDENTIAL", "loc_type"),
                            ("PERMITTYPEPERMITTYPE", "permit_type"),
                            ("EFFECTIVEDATE", "effective_date"),
                            ("EXPIRATIONDATE", "exp_date"),
                            ("WORKDATESTO", "work_date_start"),
                            ("WORKDATESFROM", "work_date_stop"),
                            ("COMPLETEDDATE", "date_end"),
                            ("SPECIALPERMITINSTRUCTIONS", "special_instructions"),
                            ("NUMBEROFMACHINES", "num_machines"),
                            ("NUMBEROFDUMPSTERS", "num_dumpsters"),
                            ("NOPARKINGAUTHORIZATION", "no_park_auth")]

        data_type_changes = [("id", "str"), ("parent_id", "str"), ("street_close_obj_id", "str"),
                             ("applicant_obj_id", "str"), ("closure_job_id", "str")]

        bool_changes = [("no_park_auth", "Y", "N", False), ("sidewalk", "Y", "N", False),
                        ("full_st_close", "Y", "N", False), ("calc_closure", "Y", "N", False),
                        ("metered_parking", "Y", "N", False), ("print_on_permit", "Y", "N", False),
                        ("travel_lane", "Y", "N", False), ("validated", "Y", "N", False)]

        # TODO: WRITE A DOC STRING FOR OUTLIER FUNC WHEN FULLY CONFIDENT
        # Because the data are hand entered in the CX system, occassionally bizarre (and extreme) values can be
        # input. On the surface this is not terribly problematic. However, outlier variables can cause runtime
        # exceptions if they exceed range limitations (for example a field that is an int receiving a 64 bit value).
        # We will detect extreme values present in the following fields and Null them as appropriate
        outliers_conv = [("num_dumpsters", MIN_DUMPSTERS, MAX_DUMPSTERS), ("num_machines", MIN_MACHINES, MAX_MACHINES)]

        # fields to convert and append with EST and UNIX from UTC timestamps

        time_conv = [("effective_date", "UTC"), ("exp_date", "UTC"), ("work_date_start", "UTC"),
                     ("work_date_stop", "UTC"), ("date_end", "UTC"), ('street_close_date_start', "UTC"),
                     ('street_close_stop', "UTC"), ('street_close_wkday_hours', "UTC"),
                     ('street_close_wkend_hours', "UTC")]

        gmap_key = os.environ["GMAP_API_KEY"]
        loc_names = {"address_field": "address", "lat_field": "lat", "long_field": "long"}

        lines = p | ReadFromText(known_args.input, coder = JsonCoder())

        load = (
                lines
                | beam.ParDo(ParseNestedFields())
                | beam.ParDo(SwapFieldNames(field_name_swaps))
                | beam.ParDo(ChangeDataTypes(data_type_changes))
                | beam.ParDo(ConvertBooleans(bool_changes, include_defaults = True))
                | beam.ParDo(FilterOutliers(outliers_conv))
                # ToDo: Modify the doc string
                | beam.ParDo(StandardizeTimes(time_conv, del_old_cols = True))
                | beam.ParDo(GoogleMapsClassifyAndGeocode(key = gmap_key, loc_field_names = loc_names,
                                                          partitioned_address = False, contains_pii = False,
                                                          del_org_input = True))
                | WriteToAvro(known_args.avro_output, schema = avro_schema, file_name_suffix = '.avro',
                              use_fastavro = True))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
