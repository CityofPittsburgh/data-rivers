# deploy_jar_files:
# 		gsutil cp -r ./dags/*.jar gs://us-east1-data-rivers-8605f7ab-bucket/dags && \
	gsutil cp -r ./dags/dependencies/gcs_loaders/*.jar gs://us-east1-data-rivers-8605f7ab-bucket/dags/dependencies/gcs_loaders


# deploy_plugins:
# 	gsutil cp -r ./plugins/*.py gs://us-east1-data-rivers-8605f7ab-bucket/plugins && \
# 	gsutil cp -r ./plugins/hooks/*.py gs://us-east1-data-rivers-8605f7ab-bucket/plugins/hooks && \
# 	gsutil cp -r ./plugins/operators/*.py gs://us-east1-data-rivers-8605f7ab-bucket/plugins/operators

# deploy_af2_dags_data_rivers:
#     gsutil -m cp -r ./af2_dags/*.py gs://us-east4-cx-test-af2-ver8-53364719-bucket/dags && \
#     gsutil -m cp -r ./af2_dags/dependencies/*.py gs://us-east4-cx-test-af2-ver8-53364719-bucket/dags/dependencies && \
#     gsutil -m cp -r ./af2_dags/dependencies/gcs_loaders/*.py gs://us-east4-cx-test-af2-ver8-53364719-bucket/dags/dependencies/gcs_loaders && \
#     gsutil -m cp -r ./af2_dags/dependencies/dataflow_scripts/*.py gs://us-east4-cx-test-af2-ver8-53364719-bucket/dags/dependencies/dataflow_scripts && \
#     gsutil -m cp -r ./af2_dags/dependencies/dataflow_scripts/dataflow_utils/*.py gs://us-east4-cx-test-af2-ver8-53364719-bucket/dags/dependencies/dataflow_scripts/dataflow_utils && \
#     gsutil -m cp -r ./af2_dags/dependencies/bq_queries/* gs://us-east4-cx-test-af2-ver8-53364719-bucket/dags/dependencies/bq_queries/


deploy_af2_dags_data_rivers_testing:
	gsutil -m cp -r ./af2_dags/*.py gs://us-east4-data-rivers-testin-bd9ba943-bucket/dags && \
	gsutil -m cp -r ./af2_dags/dependencies/*.py gs://us-east4-data-rivers-testin-bd9ba943-bucket/dags/dependencies && \
	gsutil -m cp -r ./af2_dags/dependencies/gcs_loaders/*.py gs://us-east4-data-rivers-testin-bd9ba943-bucket/dags/dependencies/gcs_loaders && \
	gsutil -m cp -r ./af2_dags/dependencies/dataflow_scripts/*.py gs://us-east4-data-rivers-testin-bd9ba943-bucket/dags/dependencies/dataflow_scripts && \
	gsutil -m cp -r ./af2_dags/dependencies/dataflow_scripts/dataflow_utils/*.py gs://us-east4-data-rivers-testin-bd9ba943-bucket/dags/dependencies/dataflow_scripts/dataflow_utils && \
    gsutil -m cp -r ./af2_dags/dependencies/bq_queries/* gs://us-east4-data-rivers-testin-bd9ba943-bucket/dags/dependencies/bq_queries/
