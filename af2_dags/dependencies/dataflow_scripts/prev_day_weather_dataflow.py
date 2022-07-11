from __future__ import absolute_import
import os
import logging
import apache_beam as beam
from apache_beam.io import ReadFromText, WriteToText
from dataflow_utils.dataflow_utils import generate_args, JsonCoder, SwapFieldNames

def run(argv = None):

    known_args, pipeline_options, avro_schema = generate_args(
            job_name = 'prev-day-weather-dataflow',
            bucket = f"{os.environ['GCS_PREFIX']}_weather",
            argv = argv,
            schema_name = 'prev_day_weather',
            limit_workers = [False, None]
    )

    with beam.Pipeline(options = pipeline_options) as p:
        field_name_swaps = [("ID", "record_id")]

        lines = p | ReadFromText(known_args.input, coder=JsonCoder())
        load = (
            lines
            | beam.ParDo(SwapFieldNames(field_name_swaps))
            | WriteToText(file_path_prefix=f"{known_args.avro_output}/test_output",
                          file_name_suffix=".txt")
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()