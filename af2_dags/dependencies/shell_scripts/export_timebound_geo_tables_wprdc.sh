#!/bin/bash

# get all the tables in the timebound_geography_datasete
# loop thru all tables and set vars to make the export statement more readable
# export all tables as csv files to the wprdc bucket
for table in $(bq ls "$GCLOUD_PROJECT:timebound_geography"| tail -n +3 | awk '{print $1}')
do

  target_table="timebound_geography.$table"
  export_csv="gs://pghpa_wprdc/timebound_geography/$table.csv"
  bq extract $target_table $export_csv
done