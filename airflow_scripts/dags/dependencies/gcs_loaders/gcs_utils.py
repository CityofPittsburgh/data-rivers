import os
import ndjson

from datetime import datetime, timedelta
from google.cloud import storage

YESTERDAY = datetime.combine(datetime.today() - timedelta(1), datetime.min.time())
WEEK_AGO = datetime.combine(datetime.today() - timedelta(7), datetime.min.time())
dt = datetime.now()

GOOGLE_APPLICATION_CREDENTIALS = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
storage_client = storage.Client()
