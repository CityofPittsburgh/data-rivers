import os
import argparse

from gcs_utils import call_odata_api_error_handling, json_to_gcs, write_partial_api_request_results_for_inspection


parser = argparse.ArgumentParser()
parser.add_argument('--output_arg', dest = 'out_loc', required = True,
                    help = 'fully specified location to upload the json file')
args = vars(parser.parse_args())

bucket = f"{os.environ['GCS_PREFIX']}_computronix"


# There is currently (11/22) an error in the CX system where the SHADOWJOB entity has no navigation property
# connecting to the various permit tables. Since several fields in the SHADOWJOB entity need to be joined into the
# other CX tables, the entire table will be pulled here and inner joined in BigQuery in a subsequent step
# CX ODATA API URL base
url = 'https://staff.onestoppgh.pittsburghpa.gov/pghprod/odata/odata/'
odata_url = F"{url}SHADOWJOB?"

# basic url to count the total number of records in the outermost entity (useful for logging if the expected number
# of results were ultimately returned)
expec_ct_url = F"{url}SHADOWJOB/$count"

# hit the api
pipe_name =  F"{os.environ['GCLOUD_PROJECT']} computronix shadow jobs"
shadow_jobs, error_flag = call_odata_api_error_handling(targ_url = odata_url, pipeline = pipe_name,
                                                       ct_url = expec_ct_url, time_out = 7200)

# load data into GCS
# out loc = <dataset>/<full date>/<run_id>_shadow_jobs.json
if not error_flag:
    json_to_gcs(args["out_loc"], shadow_jobs, bucket)
else:
    write_partial_api_request_results_for_inspection(shadow_jobs, "shadow_jobs")
