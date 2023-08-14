from __future__ import absolute_import

import argparse
import os
import io
import pandas as pd
from datetime import datetime
from google.cloud import storage
from pandas_utils import gcs_to_df

bucket = f"{os.environ['GCLOUD_PREFIX']}_ceridian"

parser = argparse.ArgumentParser()
parser.add_argument('--input', dest='input', required=True,
                    help='fully specified location of source file')
args = vars(parser.parse_args())

df = gcs_to_df(bucket, f"{args['input']}")
