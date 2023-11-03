from __future__ import absolute_import

import logging
import os
import psutil as ps

import pandas as pd

from dask import dataframe as dd


import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.dataframe.convert import to_dataframe
from apache_beam.dataframe.convert import to_pcollection
from apache_beam.io.avroio import WriteToAvro

# import util modules.
# util modules located one level down in directory (./dataflow_util_modules/datflow_utils.py)
from dataflow_utils.dataflow_utils import JsonCoder, SwapFieldNames, ChangeDataTypes, FilterFields, generate_args


DEFAULT_DATAFLOW_ARGS = [
        '--save_main_session',
        f"--project={os.environ['GCLOUD_PROJECT']}",
        f"--service_account_email={os.environ['SERVICE_ACCT']}",
        f"--region={os.environ['REGION']}",
        f"--subnetwork={os.environ['SUBNET']}"
]


# constant which dictates the ratio of available memory to data size. dataframes are faster/easier for tabular
# processing. if the dataset is smaller it faster to use in memory processing (e.g. pandas). if the dataset size
# grows then it can be faster to a distributed/lazy processing framework (dask).
# the code will adaptively choose dask OR pandas based on memory usage/availability
# the threshold below dictates the threshold for switching away from in memory processing (e.g. a 3:1 ratio of free
# memory to data size). For dask usage, the dataframe partitions are set here as well.
MEM_THRESHOLD = 3
DF_PARTITIONS = 5


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
        name_swaps = [
                ("JOBID", "job_id"),
                ("STATUSDESCRIPTION", "status"),
                ("COMMERCIALORRESIDENTIAL", "commercial_residential"),
                ("COMPLETEDDATE", "completed_date"),
                ("ISSUEDATE", "issue_date"),
                ("EXTERNALFILENUM", "ext_file_num"),
                ("PERMITWORKSCOPEXREF", "work_scope")]
        keep_cols = ['job_id', 'status', 'commercial_residential',
                        'completed_date', 'issue_date', 'ext_file_num', 'work_scope']
        type_changes = [('job_id', 'str')]

        lines = p | ReadFromText(known_args.input, coder = JsonCoder())
        load = (
                lines
                | beam.ParDo(SwapFieldNames(name_swaps))
                | beam.ParDo(FilterFields(keep_cols, exclude_target_fields = False))
                | beam.ParDo(ChangeDataTypes(type_changes))

        )
        # load the beam dataset into a pandas dataframe. use dask if memory load is too extreme. dask df maintains
        # the same name but becomes delayed df
        permit_df = to_dataframe(load)

        free_mem = ps.virtual_memory().free
        mem_usage = permit_df.__sizeof__()
        mem_ratio = free_mem / mem_usage
        if mem_ratio > MEM_THRESHOLD:
            use_in_mem = True
            print("using PANDAS")
        else:
            use_in_mem = False
            # compute chunking size
            # size/optimal  size = # chunk
            permit_df = dd.from_pandas(permit_df, npartitions = DF_PARTITIONS)
            print("using DASK")

        # filter records
        # filtering solar records requires searching for the word "solar", which is nested inside a dictionary.
        # rather than searching key-value pairs for specific entries, just convert the entire dict to a string and
        # search for the presence of "solar"
        mask_complete_status = permit_df["status"] == "Completed"
        scope = permit_df["work_scope"].astype(str)
        mask_solar = pd.Series(s.lower().__contains__("solar") for s in scope)
        filt_permit_df = permit_df[mask_complete_status & mask_solar]

        # extract the parcel number and address
        # this info is located within a nest of dicts.the most efficient approach is to flatten the json-like data
        # and extract via list comprehensions
        info = pd.json_normalize(filt_permit_df["JOBPARCELXREF"])[0].to_list()
        parc_num = ["None" if i is None else i["PARCEL.PARCELNUMBER"] for i in info]
        address = ["None" if i is None else i['PARCEL.ADDRESSABLEOBJEFORMATTEDADDRES'] for i in info]

        # if using pandas (in_mem) then copy filtered dataframe (enforce immutability), then insert location
        # columns below. if using dask then compute the delayed df into a typical pandas df (compute will assign a
        # new memory address and maintain immutability). location information is added below.
        if use_in_mem:
            processed_records_df = filt_permit_df.copy()
        else:
            processed_records_df = filt_permit_df.compute()

        processed_records_df["parc_num"] = parc_num
        processed_records_df["address"] = address
        processed_records_df.drop("JOBPARCELXREF", axis=1, inplace=True)

        # convert back to PCollection and write to AVRO
        processed_pc = to_pcollection(processed_records_df, include_indexes=True)
        | WriteToAvro(known_args.avro_output, schema = avro_schema, file_name_suffix = '.avro', use_fastavro = True)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()

