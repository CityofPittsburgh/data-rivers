from __future__ import absolute_import

import collections
import logging
import json
import re
import tempfile
import unittest

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
