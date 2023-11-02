from __future__ import absolute_import

import logging
import os
import psutil as ps
import ndjson

import pandas as pd

import dask
from dask import distributed
from dask.distributed import Client
import dask.bag as db

from dask import dataframe as dd

from google.cloud import storage

# import apache_beam as beam
# from apache_beam.io import ReadFromText
# from apache_beam.io.avroio import WriteToAvro

# import util modules.
# util modules located one level down in directory (./dataflow_util_modules/datflow_utils.py)
# from dataflow_utils.dataflow_utils import JsonCoder, SwapFieldNames, StandardizeTimes, StripStrings, FilterFields, \
#     generate_args


# dask_client = Client(n_workers=4)
storage_client = storage.Client()


# DEFAULT_DATAFLOW_ARGS = [
#         '--save_main_session',
#         f"--project={os.environ['GCLOUD_PROJECT']}",
#         f"--service_account_email={os.environ['SERVICE_ACCT']}",
#         f"--region={os.environ['REGION']}",
#         f"--subnetwork={os.environ['SUBNET']}"
# ]

# rename columns
NEW_NAMES = {"JOBID"                  : "job_id",
             "STATUSDESCRIPTION"      : "status",
             "COMMERCIALORRESIDENTIAL": "commercial_residential",
             "COMPLETEDDATE"          : "completed_date",
             "ISSUEDATE"              : "issue_date",
             "EXTERNALFILENUM"        : "ext_file_num",
             "PERMITWORKSCOPEXREF"    : "work_scope"
             }
KEPT_COLUMNS = ['job_id', 'status', 'commercial_residential',
                'completed_date', 'issue_date', 'ext_file_num', 'work_scope']

# constant which dictates the ration of available memory to data size. dataframes are faster/easier for tabular
# processing involving simple computations on smaller datasets and avoiding google managed dataflow pipelines is
# advantegous in these scenarios. this pipeline will use dataframes for the foreseeable futre (10/23). below,
# the code will adaptively choose dask if the dataset is too large for efficient in memory processing, or pandas if
# not (pandas is faster if the dataset is smaller). the threshold below dictates the threshold for switching away
# from in memory processing (e.g. a 5:1 ratio of free memory to data size).
MEM_THRESHOLD = 3



def extract_vals(x):
    # filter the unnecessary records. the table needed to filter these records isn't accessible through this api
    # currently (10/23) and the filtering must be done here
    if x:
        x_str = str(x[0])
        return x_str.lower().__contains__("solar")
    else:
        return False


def ingest_gcs_to_df():
    """ingest the raw ndjson of data extracted from cx API. determine the available memory and determine if it is
    better use dask or pandas based on permit's size vs available memory (in bytes). compute the size of the ingested data after
    loading from gcs"""

    bucket_name = F"{os.environ['GCS_PREFIX']}_computronix"
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.get_blob("solar_panel_permits/x.json")
    permits = ndjson.loads(blob.download_as_string().decode('utf-8'))

    free_mem = ps.virtual_memory().free
    mem_usage = permits.__sizeof__()
    mem_ratio = free_mem / mem_usage

    pd_df = pd.DataFrame(permits)
    mask_complete_status = pd_df["STATUSDESCRIPTION"] == "Completed"
    pd_df = pd_df[mask_complete_status]
    if mem_ratio > MEM_THRESHOLD:
        use_in_mem = True
        output_df = pd_df.rename(columns = NEW_NAMES)
        print("using PANDAS")
    else:
        use_in_mem = False
        # compute chunking size
        # size/optimal  size = # chunk
        pd_df.rename(columns = NEW_NAMES, inplace = True)
        output_df = dd.from_pandas(pd_df, npartitions = 10)
        print("using DASK")

    return output_df, use_in_mem


def process_df():
    permit_df, use_in_mem = ingest_gcs_to_df()

    solar = permit_df[permit_df["work_scope"].map(lambda x: extract_vals(x))].copy()

    solar["job_id"] = solar["job_id"].astype(str)

    info = pd.json_normalize(solar["JOBPARCELXREF"])[0].to_list()
    parc_num = ["None" if i is None else i["PARCEL.PARCELNUMBER"] for i in info]
    address = ["None" if i is None else i['PARCEL.ADDRESSABLEOBJEFORMATTEDADDRES'] for i in info]

    if use_in_mem:
        clean_records = solar[KEPT_COLUMNS]
    else:
        clean_records = solar[KEPT_COLUMNS].compute()

    clean_records["parc_num"] = parc_num
    clean_records["address"] = address

    return clean_records


# %timeit
# x = process_df()


# run function is called at the bottom of the script and the entire operation is defined within
# generate_args will initialize all options/args needed to execute the pipeline. known_args contains the runtime
# params passed in from DAG (input/output). pipeline_options contains all the flags that are initialized by default (
# project/service_acct/etc.). The schema is loaded as a dict
def run(argv = None):
    known_args, pipeline_options, avro_schema = generate_args(
            job_name = 'computronix-solar-panel-permits',
            bucket = '{}_computronix'.format(os.environ['GCS_PREFIX']),
            argv = argv,
            schema_name = 'computronix_solar_permits',
            default_arguments = DEFAULT_DATAFLOW_ARGS
    )

    with beam.Pipeline(options = pipeline_options) as p:
        # list all the table names in the order they are nested:nt1 -> xref1 -> nt2


        # lines = p | ReadFromText(known_args.input, coder = JsonCoder())
        # load = (
        #         lines
        #         | beam.ParDo()
        #
        #         | beam.ParDo(StandardizeTimes(times, t_format = "%m/%d/%Y"))
        #
        #         | WriteToAvro(known_args.avro_output, schema = avro_schema, file_name_suffix = '.avro',
        #                       use_fastavro = True))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()

