deploy:
	gsutil cp -r ./dags/*.py gs://us-east1-data-rivers-8605f7ab-bucket/dags && \
	gsutil cp -r ./plugins/*.py gs://us-east1-data-rivers-8605f7ab-bucket/plugins && \
	gsutil cp -r ./dags/*.jar gs://us-east1-data-rivers-8605f7ab-bucket/dags