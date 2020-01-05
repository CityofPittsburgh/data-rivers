from __future__ import absolute_import

import collections
import logging
import json
import re
import tempfile
import unittest

from apache_beam.io import filebasedsource

from dataflow_utils.dataflow_utils import download_schema

def create_temp_file(self, contents):
    with tempfile.NamedTemporaryFile(delete=False) as f:
        f.write(contents.encode('utf-8'))
        return f.name


def get_schema(filename):
    download_schema('pghpa_avro_schemas', filename, filename)
    SCHEMA_PATH = filename
    avro_schema = json.loads(open(SCHEMA_PATH).read())
    return avro_schema


def set_up():
    # Reducing the size of thread pools. Without this test execution may fail in
    # environments with limited amount of resources.
    filebasedsource.MAX_NUM_THREADS_FOR_SIZE_ESTIMATION = 2