deploy:
	gsutil -m cp ./dags/* gs://us-east1-data-rivers-8605f7ab-bucket/dags && \
	gsutil -m cp ./plugins/* gs://us-east1-data-rivers-8605f7ab-bucket/plugins