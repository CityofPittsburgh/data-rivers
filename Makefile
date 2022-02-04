deploy_dags:
	gsutil cp -r ./dags/*.py gs://us-east1-data-rivers-8605f7ab-bucket/dags && \
	gsutil cp -r ./dags/dependencies/*.py gs://us-east1-data-rivers-8605f7ab-bucket/dags/dependencies && \
	gsutil cp -r ./dags/dependencies/gcs_loaders/*.py gs://us-east1-data-rivers-8605f7ab-bucket/dags/dependencies/gcs_loaders && \
	gsutil cp -r ./dags/dependencies/dataflow_scripts/*.py gs://us-east1-data-rivers-8605f7ab-bucket/dags/dependencies/dataflow_scripts && \
	gsutil cp -r ./dags/dependencies/dataflow_scripts/dataflow_utils/*.py gs://us-east1-data-rivers-8605f7ab-bucket/dags/dependencies/dataflow_scripts/dataflow_utils
	
deploy_jar_files:
		# gsutil cp -r ./dags/*.jar gs://us-east1-data-rivers-8605f7ab-bucket/dags && \

	gsutil cp -r ./dags/dependencies/gcs_loaders/*.jar gs://us-east1-data-rivers-8605f7ab-bucket/dags/dependencies/gcs_loaders 


deploy_plugins:	
	# gsutil cp -r ./plugins/*.py gs://us-east1-data-rivers-8605f7ab-bucket/plugins && \
	
	gsutil cp -r ./plugins/hooks/*.py gs://us-east1-data-rivers-8605f7ab-bucket/plugins/hooks && \
	gsutil cp -r ./plugins/operators/*.py gs://us-east1-data-rivers-8605f7ab-bucket/plugins/operators
	

deploy_af2_dags:
	gsutil cp -r ./af2_dags/*.py gs://us-east1-data-rivers-8605f7ab-bucket/dags && \
	gsutil cp -r ./af2_dags/dependencies/*.py gs://us-east1-data-rivers-8605f7ab-bucket/dags/dependencies && \
	gsutil cp -r ./af2_dags/dependencies/gcs_loaders/*.py gs://us-east1-data-rivers-8605f7ab-bucket/dags/dependencies/gcs_loaders && \
	gsutil cp -r ./af2_dags/dependencies/dataflow_scripts/*.py gs://us-east1-data-rivers-8605f7ab-bucket/dags/dependencies/dataflow_scripts && \
	gsutil cp -r ./af2_dags/dependencies/dataflow_scripts/dataflow_utils/*.py gs://us-east1-data-rivers-8605f7ab-bucket/dags/dependencies/dataflow_scripts/dataflow_utils
