import json
import os
from io import StringIO
import argparse

import pandas as pd
import numpy as np

import geojson
from geojson import FeatureCollection, LineString
from geojson import Feature
from google.cloud import storage


parser = argparse.ArgumentParser()
parser.add_argument('--input_bucket', dest = 'input_bucket', required = True)
parser.add_argument('--input_blob', dest = 'input_blob', required = True)
parser.add_argument('--output_bucket', dest = 'output_bucket', required = True)
args = vars(parser.parse_args())


# retrieve input csv from GCS. String IO operation tranforms bytes to string
dl_storage_client = storage.Client(project = "data-bridGIS")
bucket_name = args["input_bucket"]
bucket = dl_storage_client.bucket(bucket_name)
blob = bucket.blob(args["input_blob"])
blob = blob.download_as_string()
blob = blob.decode('utf-8')
blob = StringIO(blob)


# ingest raw csv (which is in string format) into DF (pulled from GCS)
data_in = pd.read_csv(blob, sep=",")


# Cleaning operation 1:
# convert existing "nan" geometry values (which are strings) to numpy nulls and reverse the coordinates of each point
# as geojson uses x, y format as opposed lat/long
g = data_in["geometry"].to_list()
new_geo = []
for val in g:
    if "[nan, nan]" in val:
        new_geo.append(np.nan)
    else:
        coords = json.loads(val)
        for c in coords:
            c.reverse()
        new_geo.append(coords)


# Cleaning operation 2: drop rows with null geometry (these can't be plotted) & insert the reversed geometry
data_in_clean = pd.DataFrame(data_in.drop("geometry", axis = 1).copy(), dtype=object)
data_in_clean["geometry"] = new_geo
data_in_clean.dropna(subset = ["geometry"], inplace = True)
data_in_clean = data_in_clean.reset_index(drop = True)


# Extract column names for assigning features dict in next step (don't use geo as that is handled separately)
feature_columns = data_in_clean.columns.to_list()
feature_columns.remove("geometry")


# loop through DF and initialize the json properties/geo for writing in next step (this is essentially manual
# construction of the geojson as opposed to built-in geopandas function. This was easier than conversion/construction
# of the reversed LineString points and is very fast overall
features = []
for i in range(len(data_in_clean)):
    row = data_in_clean.loc[i]
    features.append(
           geojson.Feature(
                   geometry = LineString(row["geometry"]),
                   properties = row[feature_columns].astype("str").to_dict()
           )
    )


# Write the json to GCS
ul_storage_client = storage.Client()
upload_blob = storage.Blob(name="closures", bucket=ul_storage_client.get_bucket(args["output_bucket"]))
upload_blob.upload_from_string(
            data=json.dumps(FeatureCollection(features)),
            content_type='application/json',
            client = ul_storage_client
        )