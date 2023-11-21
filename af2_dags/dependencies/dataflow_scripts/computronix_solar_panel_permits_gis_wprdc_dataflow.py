from __future__ import absolute_import

import logging
import os

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.avroio import WriteToAvro

# import util modules.
# util modules located one level down in directory (./dataflow_util_modules/datflow_utils.py)
from dataflow_utils.dataflow_utils import JsonCoder, SwapFieldNames, ChangeDataTypes, FilterFields, generate_args, get_schema


DEFAULT_DATAFLOW_ARGS = [
        '--save_main_session',
        f"--project={os.environ['GCLOUD_PROJECT']}",
        f"--service_account_email={os.environ['SERVICE_ACCT']}",
        f"--region={os.environ['REGION']}",
        f"--subnetwork={os.environ['SUBNET']}"
]


# run function is called at the bottom of the script and the entire operation is defined within
# generate_args will initialize all options/args needed to execute the pipeline. known_args contains the runtime
# params passed in from DAG (input/output). pipeline_options contains all the flags that are initialized by default (
# project/service_acct/etc.). The schema is loaded as a dict
# Note: As of 11/23 extensive testing has revealed that in the testing env it is faster to run all transformation
# though dataflow/BEAM ParDo operations. Pandas, DASK, and BEAM dataframes were all considered in the testing.


def mask_comp_solar(datum):
    wk_scope = str(datum["PERMITWORKSCOPEXREF"])
    mask = datum['STATUSDESCRIPTION'] == 'Completed' and (wk_scope.lower().__contains__("solar"))
    return mask

class ExtractLocData(beam.DoFn):
    def __init__(self, loc_field):
        """
        :param loc_field: list of single dict containing nested location related information to extract
        """
        self.loc_field = loc_field

    def process(self, datum):
        if datum[self.loc_field]:
            loc = datum[self.loc_field][0]["PARCEL"]
            datum["parc_num"] = loc["PARCELNUMBER"]
            datum["address"] = loc["ADDRESSABLEOBJEFORMATTEDADDRES"]
        else:
            datum["parc_num"] = None
            datum["address"] = None
        yield datum


def run(argv = None):
    print("starting df pipe")

    known_args, pipeline_options, avro_schema = generate_args(
            job_name = 'computronix-solar-panel-permits',
            bucket = '{}_computronix'.format(os.environ['GCS_PREFIX']),
            argv = None,
            schema_name = 'computronix_solar_permits',
            default_arguments = DEFAULT_DATAFLOW_ARGS
    )


    with beam.Pipeline(options = pipeline_options) as p:
        name_swaps = [
                ("JOBID", "job_id"),
                ("STATUSDESCRIPTION", "status"),
                ("COMMERCIALORRESIDENTIAL", "commercial_residential"),
                ("COMPLETEDDATE", "completed_date"),
                ("ISSUEDATE", "issue_date"),
                ("EXTERNALFILENUM", "ext_file_num"),
                ("PERMITWORKSCOPEXREF", "work_scope")
        ]
        keep_cols = ['job_id', 'status', 'commercial_residential', 'completed_date', 'issue_date', 'ext_file_num',
                     'work_scope', "parc_num", "address"]
        type_changes = [('job_id', 'str'), ('work_scope', 'str')]

        lines = p | ReadFromText(known_args.input, coder=JsonCoder())

        load = (
                lines
                | beam.Filter(mask_comp_solar)
                | beam.ParDo(ExtractLocData("JOBPARCELXREF"))
                | beam.ParDo(SwapFieldNames(name_swaps))
                | beam.ParDo(FilterFields(keep_cols, exclude_target_fields = False))
                | beam.ParDo(ChangeDataTypes(type_changes))
                | WriteToAvro(known_args.avro_output, schema = avro_schema, file_name_suffix = '.avro', use_fastavro = True)
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()


