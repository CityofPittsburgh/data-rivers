import os
import argparse
from google.cloud import storage

from gcs_utils import json_to_gcs, get_ceridian_report

storage_client = storage.Client()
bucket = f"{os.environ['GCS_PREFIX']}_ceridian"

parser = argparse.ArgumentParser()
parser.add_argument('--output_arg', dest='out_loc', required=True,
                    help='fully specified location to upload the combined ndjson file')
args = vars(parser.parse_args())

all_records = get_ceridian_report('JOBTITLES')
print(f"{len(all_records)} job titles extracted")
json_to_gcs(f"{args['out_loc']}", all_records, bucket)
