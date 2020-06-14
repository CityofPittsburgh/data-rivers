# Data Rivers
Apache Airflow + Beam ETL scripts for the City of Pittsburgh's data pipelines, orchestrated with Google Cloud Composer.

Requirements: Python 3.7.x, access to city GCP account, JSON file with Data Rivers service account key (stored in the project root as `data_rivers_key.json` but excluded from version control for security reasons via `.gitignore`)

[Apache Airflow documentation](https://airflow.apache.org/docs/stable/)

[Apache Beam documentation](https://beam.apache.org/documentation/)

## Architecture
The [DAGs](https://airflow.apache.org/docs/stable/concepts.html#dags) in this project share a common structure. First, they load data (either via queries to third-party APIs or on-prem JDBC databases) to Google Cloud Storage, in JSON or CSV format. For many data sources (particularly those on-prem), there exist R scripts from our legacy system to do the data extraction. To avoid duplicating this work, we've packaged a number of these scripts in Docker images stored privately on Google Container Registry. We use Airflow's `DockerOperator` to invoke those scripts, which in some cases return CSV and in others newline-delimited JSON (Beam requires newline-delimited). 

Where legacy loading scripts don't exist, we load data via Python scripts [stored in this project](https://github.com/CityofPittsburgh/airflow_scripts/tree/master/airflow_scripts/dags/dependencies/gcs_loaders). Whenever possible, we load data as newline-delimited JSON, which is easier to work with downstream. We use Python's `datetime` library to bucket these files by time (e.g. `gs://pghpa_computronix/businesses/2020/02/2020-02-07_business_licenses.json`).

Once files are loaded to Cloud Storage, we run [Apache Beam scripts](https://github.com/CityofPittsburgh/airflow_scripts/tree/master/airflow_scripts/dags/dependencies/dataflow_scripts) via Dataflow, Google's hosted Beam offering. These scripts transform the data where necessary (e.g. changing data types, normalizing addresses) and convert it to [avro](https://avro.apache.org/docs/current/). Avro has several advantages: its fast and compact binary format; its self-describing schemas, which provide type safety; and its status as the preferred format for loading to BigQuery. Avro schemas are defined as JSON, and can include an arbitrary number of additional fields beyond data types, so they're useful for storing human-readable metadata (e.g. data steward, publish frequency) along with schema information. 

The Dataflow scripts validate datums against avro schemas, which we push to [this repository](https://github.com/CityofPittsburgh/data-rivers-schemas/) and copy to a Google Cloud Storage bucket. When creating a DAG for a new data source, you'll first need to create the avro schema, push it to the `data-rivers-schemas` repo, and copy it to the `pghpa_avro_schemas` bucket. Note that geocoding/reverse geocoding attributes, or other enrichments added via BigQuery joins, aren't part of these schemas, as they aren't added until later in the pipeline. Where applicable, we add metadata in the avro schemas (e.g. `"reverse_geocoded": "True"` or `"downstream_enrichments": "["foo", "bar"]`) to flag additional fields that appear in the final BigQuery table.

The Dataflow scripts read the schemas from that bucket. Once any necessary transforms are completed and the data has been converted to avro, we upload it to a separate folder (`avro_output`) in the same bucket that houses the source JSON or CSV. Again, these filenames are time-bucketed using the `datetime` module.

Once the avro files are in Cloud Storage, we move to the next step in the Airflow DAG: using Airflow's `GoogleCloudStoragetoBigQuery` operator to (you guessed it) insert the data into BigQuery. In cases where the data needs to be geocoded or reverse-geocoded, it's loaded into a temporary table. Subsequent DAG steps then use the `BigQueryOperator` and queries built via [helper functions](https://github.com/CityofPittsburgh/airflow_scripts/blob/master/airflow_scripts/dags/dependencies/airflow_utils.py#L60-L128) to join the data with location information (e.g. neighborhood, council district, police zone) in another temporary table. Finally, that temporary table is appended to the final table. 


## Running locally
As a start, you'll need to [install and configure](https://airflow.apache.org/docs/stable/installation.html) Apache Airflow. 
Once you've done so, you can trigger tasks or entire DAGs locally via `airflow run my_dag my_task YYYY-MM-DD` or `airflow trigger_dag my_dag`. To view progress/logging in the Airflow web UI, you'll need to run the processes `airflow webserver` (which shows on port 8080) and `airflow scheduler` in two separate terminal windows. Make sure you have the proper `.env` values available in the shell sessions from which you run those commands, and that you've activated this project's virtualenv in those sessions.

Note that when running DAGs locally that use a `DockerOperator` or `GKEPodOperator` (the GCP flavor of Docker), the jobs will fail if you haven't pulled the relevant images from Google Container Registry to your machine. Example pull command: `docker pull gcr.io/data-rivers/pgh-trash-can-api`

A lot of these images are pretty bulky, so if you're working on a machine with limited disk space, it may make sense to delete them (via the `docker rmi` command) once you've completed your work, then re-pull as needed.

Consult `env.example` for the necessary environment variables (talk to James or Sihan to get these). We recommend installing [`autoenv`](https://github.com/inishchith/autoenv) via homebrew so that your system sources environment variables automatically when you enter the directory.

You'll see that we use the variables `GCLOUD_PROJECT` and `GCS_PREFIX` throughout the scripts. In your local environment, these should be set to `data-rivers-testing` and `pghpa_test`, respectively (this is extremely important). In the production environment (hosted via Cloud Composer), the variables are set to `data-rivers` and `pghpa`. This gives us a testing sandbox for local development while walling off the production environment from code that hasn't yet been merged and deployed from `master`.  

## Tests
Write tests for every new Dataflow script. You can execute the entire test suite by running `pytest` from the project root (please do so before making any new pull requests).

Travis integration = work in progress
