import argparse

from gcs_utils import call_odata_api, json_to_gcs


parser = argparse.ArgumentParser()
parser.add_argument('--output_arg', dest = 'out_loc', required = True,
                    help = 'fully specified location to upload the json file')
args = vars(parser.parse_args())

# TODO: remove scratch bucket
# bucket = f"{os.environ['GCS_PREFIX']}_computronix"
# bucket = "pghpa_test_scratch"



# There is currently (11/22) an error in the CX system where the SHADOWJOB entity has no navigation property
# connecting to the various permit tables. Since several fields in the SHADOWJOB entity need to be joined into the
# other CX tables, the entire table will be pulled here and inner joined in BigQuery in a subsequent step
# CX ODATA API URL base
url = 'https://staff.onestoppgh.pittsburghpa.gov/pghprod/odata/odata/'
shadow_url = F"{url}SHADOWJOB?"
shadow_jobs = call_odata_api(shadow_url)


# load data into GCS
# out loc = <dataset>/<full date>/<run_id>_shadow_jobs.json
json_to_gcs(args["out_loc"], shadow_jobs, bucket)
