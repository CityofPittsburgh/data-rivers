from __future__ import absolute_import

import logging
import os

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.avroio import WriteToAvro

# import util modules.
# util modules located one level down in directory (./dataflow_util_modules/datflow_utils.py)
from dataflow_utils.dataflow_utils import JsonCoder, SwapFieldNames, ChangeDataTypes, generate_args

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
def run(argv = None):
    known_args, pipeline_options, avro_schema = generate_args(
            job_name = 'computronix-shadow-jobs',
            bucket = F'{}_computronix'.format(os.environ['GCS_PREFIX']),
            argv = argv,
            schema_name = 'computronix_shadow_jobs',
            default_arguments = DEFAULT_DATAFLOW_ARGS
    )

    with beam.Pipeline(options = pipeline_options) as p:
        name_swaps = [("OBJECTID", "obj_id"), ("REFERENCEDOBJECTID", "ref_obj_id"),
                      ("SNP_OWNER", "owner"), ("SNP_LATITUDE", "lat"),("SNP_LONGITUDE", "long"),
                      ("SNP_STREETNAME1", "street_name"), ("SNP_NEIGHBORHOOD", "neighborhood"), ("SNP_WARD", "ward"),
                      ("SNP_CENSUSTRACT", "census_tract"), ("SNP_CHDINDIVIDUALHISTORICPROP", "historic_prop"),
                      ("SNP_HISTORICDISTRICTNAME", "historic_dist_name"),
                      ("SNP_HISTORICDISTRICTTYPE", "historic_dist_type"),
                      ("SNP_COUNCILDISTRICT", "council_dist"), ("SNP_FEMAFLOODWAY", "fema_floodway"),
                      ("SNP_LANDSLIDEPRONEAREA", "landslide_area"),
                      ("SNP_STEEPSLOPE", "steep_slope"), ("SNP_UNDERMINED", "undermined"),
                      ("SNP_ZONINGDISTRICT", "zoning_dist"), ("SNP_ZONINGOVERLAY", "zoning_overlay")]

        type_changes = [('obj_id', 'int'), ('ref_obj_id', 'int')]

        lines = p | ReadFromText(known_args.input, coder = JsonCoder())

        load = (
                lines
                | beam.ParDo(SwapFieldNames(name_swaps))
                | beam.ParDo(ChangeDataTypes(type_changes))
                | WriteToAvro(known_args.avro_output, schema = avro_schema, file_name_suffix = '.avro',
                              use_fastavro = True))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
