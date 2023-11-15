# --input gs://pghpa_test_computronix/solar_panel_permits/2023/11/07/manual__2023-11-07T14:49:30.707329+00:00_solar_panel_permits.json
# --avro_output gs://pghpa_test_computronix/solar_panel_permits/test_avro/

# Hacky imports for debugging.
# TODO: clean out the junk imports Wood
from __future__ import absolute_import

import logging
import os
import collections
import typing
import ndjson

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.dataframe.convert import to_dataframe
from apache_beam.dataframe.convert import to_pcollection
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


# NEED TO USE A SCHEMA AWARE PCOLLECTION SO THAT BEAM CAN CONSTRUCT A (DEFERRED) DATAFRAME WITH THE CORRECT TYPE
# HINTS ETC
# The goal is to use normal BEAM processing for several steps of a pipeline and then pass the PCollection into a
# dataframe for a few transformations. (Note...BEAM's dataframe is very very closely modelled on many aspects of
# Pandas, but is not actually Pandas. If you need more info, read a little about Deferred Dataframe (lazy execution)
# to see some key processing differences. Again, BEAM dataframes are not true Pandas (eager execution) or DASK (lazy)
# or any other dataframe platform...they're simply very close models of the same ideas.

# When BEAM reads in NDJSON data it is not schema aware. It needs a "Coder" for this reason. To push data into a DF
# we have to make the PCollection "schema aware". One possibility is to read in the data with a schema, and another is
# to enforce the schema before transforming into a DF (but not on the read in).

# The problem is that when transforming the PCollection into a dataframe (via to_dataframe...), the schema isn't
# recognized or "placed where it should be" so to speak. The second argument of to_datafram is 'proxy'. The meaning
# of the proxy isn't clear exactly. It is not needed if you have a schema in place. However, all approaches so far
# haven't led to a place where the schema is "working" and thus, I can't bypass proxy. But...anything passed in as a
# proxy is also the wrong data type.

# The schema itself, I think, needs to take the form of a named tuple. There are at least 2 ways to instantiate
# one...the collections and typing packages (both have been tried). Below is some scrap code building one of the many
# attempts at a named tuple schema. The class PermitSchema was hard coded below to streamline things...
# Note: job_id comes in as aan int and needs to be converted to str. The schema def below may need to be altered
# depending on when you're applying the schema

# create PCollection schema
# raw_schema = get_schema('computronix_solar_permits', "")
# fds = raw_schema["fields"]
# schema_dict = {f["name"]:f["type"] for f in fds}
# collections.namedtuple("PermitSchema", list(schema_dict.keys()))
# PermitSchema = collections.namedtuple("PermitSchema", list(schema_dict.keys()))
# job_id: [str, 'null']
#     status: [str, 'null'], 'commercial_residential': ['string', 'null'],
#      'completed_date': ['string', 'null'], 'issue_date': ['string', 'null'], 'ext_file_num': ['string'],
#      'parc_num'      : ['string', 'null'], 'address': ['string', 'null']}


class PermitSchema(typing.NamedTuple):
    job_id: str
    status: str
    commercial_residential: str
    completed_date: str
    issue_date: str
    ext_file_num: str
    parc_num: str
    address: str



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

    print("starting df pipe")
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
        keep_cols = ['job_id', 'status', 'commercial_residential',
                     'completed_date', 'issue_date', 'ext_file_num', 'work_scope']
        type_changes = [('job_id', 'str')]

        lines = p | ReadFromText(known_args.input, coder = JsonCoder())

        load = (
                lines
                | beam.ParDo(SwapFieldNames(name_swaps))
                | beam.ParDo(FilterFields(keep_cols, exclude_target_fields = False))
                | beam.ParDo(ChangeDataTypes(type_changes))

                # generate an error because 'proxy' is missing with the line below
                | to_dataframe(lines)

                # OR
                # generate an error because proxy isn't the right data type with this line
                # | to_dataframe(lines, beam.Map(lambda x: beam.Map(lambda x: PermitSchema(**x))))

                # one of many attempts to enforce the schema and apply it to the PCollection. Many flavors of this
                # code were tried without much success, but only this one is preserved...
                # | beam.Map(lambda x: PermitSchema(**x)).with_output_types(PermitSchema)
                # | to_dataframe(lines)
        )


# Here lies a bunch of WORKING pandas code to perform the rest of the transforms. Commented out for now, and can be
# inserted when ready. It can run in the pipeline declared above or in a second pipe perhaps.

    # # filtering solar records requires searching for the word "solar", which is nested inside a dictionary.
    # # rather than searching key-value pairs for specific entries, just convert the entire dict to a string and
    # # search for the presence of "solar"
    # mask_complete_status = permit_df["STATUSDESCRIPTION"] == "Completed"
    # scope = permit_df["PERMITWORKSCOPEXREF"].astype(str)
    # mask_solar = pd.Series(s.lower().__contains__("solar") for s in scope)
    # filt_permit_df = permit_df[mask_complete_status & mask_solar]
    #
    # # extract the parcel number and address
    # # this info is located within a nest of dicts.the most efficient approach is to flatten the json-like data
    # # and extract via list comprehensions
    # info = pd.json_normalize(filt_permit_df["JOBPARCELXREF"])[0].to_list()
    # parc_num = ["None" if i is None else i["PARCEL.PARCELNUMBER"] for i in info]
    # address = ["None" if i is None else i['PARCEL.ADDRESSABLEOBJEFORMATTEDADDRES'] for i in info]
    #
    # # # copy filtered dataframe (enforce immutability), then insert location columns below.
    # processed_records_df = filt_permit_df.copy()
    # processed_records_df["parc_num"] = parc_num
    # processed_records_df["address"] = address
    # processed_records_df.drop("JOBPARCELXREF", axis = 1, inplace = True)


# Convert the dataframe back into a PCollection and then write to avro. Maybe it can/should be written from the df
# though I haven't looked into AVRO IO for dataframes
# | to_pcollection(processed_records_df)
# | WriteToAvro(known_args.avro_output, schema = avro_schema, file_name_suffix = '.avro', use_fastavro = True)



if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()

# BEAM dataframes are lazy execution and DASK isn't advantageous over the BEAM DF because of this. However,
# if dataframes are used BEFORE the pipeline begins, it may be helpful to use DASK vs Pandas. Code below handles on
# the fly switching

# constant which dictates the ratio of available memory to data size. dataframes are faster/easier for tabular
# processing. if the dataset is smaller it faster to use in memory processing (e.g. pandas). if the dataset size
# grows then it can be faster to a distributed/lazy processing framework (dask).
# the code will adaptively choose dask OR pandas based on memory usage/availability
# the threshold below dictates the threshold for switching away from in memory processing (e.g. a 3:1 ratio of free
# memory to data size). For dask usage, the dataframe partitions are set here as well. A theoretical 5:1 ratio of
# memory is ideal for production. Partitions in DASK were timed and tested under many conditions (early Nov 2023). 5
# is the optimum paritioning as of testing and this can be verified before using in prod
MEM_THRESHOLD = 3
DF_PARTITIONS = 5

# use dask if memory load is too extreme. dask df maintains the same name but becomes deferred df
# free_mem = ps.virtual_memory().free
# mem_usage = permit_df.__sizeof__()
# mem_ratio = free_mem / mem_usage
# if mem_ratio > MEM_THRESHOLD:
#     use_in_mem = True
#     print("using PANDAS")
# else:
#     use_in_mem = False
#     # compute chunking size
#     # size/optimal  size = # chunk
#     permit_df = dd.from_pandas(permit_df, npartitions = DF_PARTITIONS)
#     print("using DASK")
