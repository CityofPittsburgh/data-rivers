# Data Rivers
Apache Airflow + Beam ETL scripts for the City of Pittsburgh's Data Rivers project, orchestrated with Google Cloud Composer.

Requirements: Python 3.7.x, access to city GCP account

[Apache Airflow documentation](https://airflow.apache.org/docs/stable/)

[Apache Beam documentation](https://beam.apache.org/documentation/)

## Architecture
The [DAGs](https://airflow.apache.org/docs/stable/concepts.html#dags)in this project share a common structure. First, they load data (either via queries to third-party APIs or on-prem JDBC databases) to Google Cloud Storage, in JSON or CSV format. For many data sources (particularly those on-prem), there exist R scripts from our legacy system to do the data extraction. To avoid duplicating this work, we've packaged a number of these scripts in Docker images stored privately on Google Container Registry. We use Airflow's `DockerOperator` to invoke those scripts, which in some cases return CSV and in others newline-delimited JSON (Beam requires newline-delimited). 

Where legacy scripts don't exist, we load data via Python scripts [stored in this project](https://github.com/CityofPittsburgh/airflow_scripts/tree/master/airflow_scripts/dags/dependencies/gcs_loaders). Whenever possible, we load data as newline-delimited JSON, which is easier to work with downstream. We use Python's `datetime` library to bucket these files by time (e.g. `gs://pghpa_computronix/business/2020/02/2020-02-07_business_licenses.json`).

Once files are loaded to Cloud Storage, we run [Apache Beam scripts](https://github.com/CityofPittsburgh/airflow_scripts/tree/master/airflow_scripts/dags/dependencies/dataflow_scripts) via Dataflow, Google's hosted Beam offering. These scripts perform transformations on the data where necessary (e.g. converting data types, normalizing addresses) and convert it to [avro](https://avro.apache.org/docs/current/) format. Avro has several advantages: its fast and compact binary format; its self-describing schemas, which provide type safety; and status as the preferred format for loading to BigQuery. Avro schemas are defined in JSON format, and can include an arbitrary number of additional fields beyond data types, so they're useful for storing human-readable metadata (e.g. data steward, publish frequency) along with schema information. 


## Running locally
As a start, you'll need to [install and configure](https://airflow.apache.org/docs/stable/installation.html) Apache Airflow. 
Once you've done so, you can trigger tasks or entire DAGs locally via `airflow run my_dag my_task YYYY-MM-DD` or `airflow trigger_dag my_dag`. To view progress/logging in the Airflow web UI, you need to run the processes `airflow webserver` (which runs on port 8080) and `airflow scheduler`. Make sure you have the proper `.env` values available in the shell sessions from which you run those commands, and that you've activated this project's virtualenv in those sessions.

Note that when running DAGs locally that use a `DockerOperator` or `GKEPodOperator`, if you haven't pulled the relevant images from Google Container Registry to your machine, the jobs will fail. Example pull command: `docker pull gcr.io/data-rivers/pgh-trash-can-api`

A lot of these images are pretty bulky, so if you're working on a machine with limited disk space, it may make sense to delete them (via the `docker rmi` command) once you've completed your work. 

## Tests
Write tests for every new Dataflow script. You can execute the entire test suite by running `pytest` from the project root.

Travis integration = work in progress
