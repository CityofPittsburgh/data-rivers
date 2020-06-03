from __future__ import absolute_import

import tempfile
import unittest

from apache_beam.io import filebasedsource


def create_temp_file(self, contents):
    with tempfile.NamedTemporaryFile(delete=False) as f:
        f.write(contents.encode('utf-8'))
        return f.name


def set_up():
    # Reducing the size of thread pools. Without this test execution may fail in
    # environments with limited amount of resources.
    filebasedsource.MAX_NUM_THREADS_FOR_SIZE_ESTIMATION = 2