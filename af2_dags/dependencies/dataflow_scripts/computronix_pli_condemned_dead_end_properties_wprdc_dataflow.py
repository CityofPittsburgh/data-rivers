from __future__ import absolute_import

import logging
import os

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.avroio import WriteToAvro

# import util modules.
# util modules located one level down in directory (./dataflow_util_modules/datflow_utils.py)
from dataflow_utils.dataflow_utils import JsonCoder, SwapFieldNames, StandardizeTimes, StripStrings, FilterFields, \
    generate_args

DEFAULT_DATAFLOW_ARGS = [
        '--save_main_session',
        f"--project={os.environ['GCLOUD_PROJECT']}",
        f"--service_account_email={os.environ['SERVICE_ACCT']}",
        f"--region={os.environ['REGION']}",
        f"--subnetwork={os.environ['SUBNET']}"
]


class UnNestFields(beam.DoFn):
    def __init__(self, nest):
        self.nest = nest

    def process(self, datum):
        try:
            extract_1 = datum[self.nest[0]]
            extract_2 = extract_1[self.nest[1]][0][self.nest[2]]
            extract_1.pop(self.nest[1])

            datum.pop(self.nest[0])
            datum.update(extract_1)
            datum.update(extract_2)

            yield datum

        # if there are missing values then null
        except KeyError:
            datum["PARCELNUMBER"] = None
            datum["ADDRESSABLEOBJEFORMATTEDADDRES"] = None
            datum["OWNERNAME"] = None

            datum.pop("PARCEL")

            yield datum

        except TypeError:
            datum["PARCELNUMBER"] = None
            datum["ADDRESSABLEOBJEFORMATTEDADDRES"] = None
            datum["OWNERNAME"] = None

            datum.pop("PARCEL")

            yield datum

        except IndexError:
            pass


# run function is called at the bottom of the script and the entire operation is defined within
# generate_args will initialize all options/args needed to execute the pipeline. known_args contains the runtime
# params passed in from DAG (input/output). pipeline_options contains all the flags that are initialized by default (
# project/service_acct/etc.). The schema is loaded as a dict
def run(argv = None):
    known_args, pipeline_options, avro_schema = generate_args(
            job_name = 'computronix-pli-condemned-deadend-properties-wprdc',
            bucket = '{}_computronix'.format(os.environ['GCS_PREFIX']),
            argv = argv,
            schema_name = 'computronix_pli_properties_wprdc',
            default_arguments = DEFAULT_DATAFLOW_ARGS
    )

    with beam.Pipeline(options = pipeline_options) as p:
        # list all the table names in the order they are nested:nt1 -> xref1 -> nt2
        table_structure = ["PARCEL", "PARCELPARCELOWNERXREF", "PARCELOWNER"]

        name_swaps = [("LATESTINSPECTIONRESULT", "latest_inspec_result"),
                      ("LATESTINSPECTIONSCORE", "latest_inspec_score"), ("CREATEDDATE", "create_date"),
                      ("PROGINSPTYPEDESCRIPTION", "insp_type_desc"), ("PROGRAMINSPECTIONSTATUS", "insp_status"),
                      ("PARCELNUMBER", "parc_num"), ("ADDRESSABLEOBJEFORMATTEDADDRES", "address"),
                      ("OWNERNAME", "owner")]

        times = [("create_date", "EST")]

        drops = ["create_date"]

        fields_to_strip = ["insp_type_desc"]

        lines = p | ReadFromText(known_args.input, coder = JsonCoder())
        load = (
                lines
                | beam.ParDo(UnNestFields(table_structure))
                | beam.ParDo(SwapFieldNames(name_swaps))
                | beam.ParDo(StandardizeTimes(times, t_format = "%m/%d/%Y"))
                | beam.ParDo(StripStrings(fields_to_strip))
                | beam.ParDo(FilterFields(drops))
                | WriteToAvro(known_args.avro_output, schema = avro_schema, file_name_suffix = '.avro',
                              use_fastavro = True))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
