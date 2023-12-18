#!/bin/bash

for table in $(bq ls "$GCLOUD_PROJECT:timebound_geography"| tail -n +3 | awk '{print $1}')
do

  target_table="timebound_geography.$table"
  export_csv="gs://pghpa_wprdc/timebound_geography/$table.csv"
  bq extract $target_table $export_csv
done