from __future__ import absolute_import

import logging
import os

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.avroio import WriteToAvro

# import util modules.
# util modules located one level down in directory (./dataflow_util_modules/datflow_utils.py)
from dataflow_utils.dataflow_utils import JsonCoder, SwapFieldNames, StandardizeTimes, ConvertStringCase, \
    StandardizeParcelNumbers, generate_args

DEFAULT_DATAFLOW_ARGS = [
        '--save_main_session',
        f"--project={os.environ['GCLOUD_PROJECT']}",
        f"--service_account_email={os.environ['SERVICE_ACCT']}",
        f"--region={os.environ['REGION']}",
        f"--subnetwork={os.environ['SUBNET']}"
]


class UnNestFields(beam.DoFn):
    def __init__(self, nest, del_targ_fields = True):
        self.nest = nest
        self.del_targ_fields = del_targ_fields

    def process(self, datum):
        try:
            extract_outer = datum[self.nest[0]][0][self.nest[1]]
            extract_inner = extract_outer[self.nest[2]][0][self.nest[3]]
            extract_outer.pop(self.nest[2])
            extracted = extract_outer.copy()
            extracted.update(extract_inner)

            if self.del_targ_fields:
                datum.pop(self.nest[0])

            datum.update(extracted)
            yield datum

        # if there are missing values then null
        except IndexError:
            datum["FORMATTEDPARCELNUMBER"] = None
            datum["ADDRESSABLEOBJEFORMATTEDADDRES"] = None
            datum["OWNERNAME"] = None
            datum.pop(self.nest[0])
            yield datum


# run function is called at the bottom of the script and the entire operation is defined within
# generate_args will initialize all options/args needed to execute the pipeline. known_args contains the runtime
# params passed in from DAG (input/output). pipeline_options contains all the flags that are initialized by default (
# project/service_acct/etc.). The schema is loaded as a dict
def run(argv = None):
    known_args, pipeline_options, avro_schema = generate_args(
            job_name = 'computronix-pli-wprdc',
            bucket = '{}_computronix'.format(os.environ['GCS_PREFIX']),
            argv = argv,
            schema_name = 'computronix_pli_permits_wprdc',
            default_arguments = DEFAULT_DATAFLOW_ARGS
    )

    with beam.Pipeline(options = pipeline_options) as p:
        # nested expansion tables (expansion from each base table) (xrefs are cross referenced tables that are used
        # to link
        # tables together and don't contain fields of interest)
        xref_1 = "JOBPARCELXREF"
        nested_table_1 = "PARCEL"
        xref_2 = "PARCELPARCELOWNERXREF"
        nested_table_2 = "PARCELOWNER"

        # list all the table names in the order they are nested: base-> xref1 -> nt1 -> xref2 -> nt2
        table_structure = [xref_1, nested_table_1, xref_2, nested_table_2]

        name_swaps = [("EXTERNALFILENUM", "ext_file_num"), ("ISSUEDATE", "issue_date"), ("OWNERNAME", "owner_name"),
                      ("ALLCONTRACTORSNAME", "contractor_name"), ("TOTALPROJECTVALUE", "total_proj_val"),
                      ("TYPEOFWORKDESCRIPTION", "type_work"), ("COMMERCIALORRESIDENTIAL", "commercial_or_residential"),
                      ("WORKDESCRIPTION", "work_desc"), ("FORMATTEDPARCELNUMBER", "parc_num"),
                      ("ADDRESSABLEOBJEFORMATTEDADDRES", "obj_address")]

        str_convs = [("ext_file_num", "upper"), ("permit_type", "upper"), ("owner_name", "upper"),
                     ("contractor_name", "upper"), ("type_work", "upper"), ("commercial_or_residential", "upper"),
                     ("work_desc", "upper"), ("parc_num", "upper"), ("obj_address", "upper")]

        times = [("issue_date", "US/Eastern")]

        lines = p | ReadFromText(known_args.input, coder = JsonCoder())

        load = (
                lines
                | beam.ParDo(UnNestFields(table_structure))
                | beam.ParDo(SwapFieldNames(name_swaps))
                | beam.ParDo(ConvertStringCase(str_convs))
                | beam.ParDo(StandardizeTimes(times, t_format = "%m/%d/%Y"))
                | beam.ParDo(StandardizeParcelNumbers("parc_num"))
                | WriteToAvro(known_args.avro_output, schema = avro_schema, file_name_suffix = '.avro',
                              use_fastavro = True))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
