# Data Rivers
Apache Airflow + Beam ETL scripts for the City of Pittsburgh's data pipelines, orchestrated with Google Cloud Composer.

Requirements: Python 3.7.x, access to city GCP account, JSON file with Data Rivers service account key (stored in the project root as `data_rivers_key.json` but excluded from version control for security reasons via `.gitignore`)

[Apache Airflow documentation](https://airflow.apache.org/docs/stable/)

[Apache Beam documentation](https://beam.apache.org/documentation/)

## Architecture

![](https://raw.githubusercontent.com/CityofPittsburgh/data-rivers/master/data_rivers_architecture.png?raw=true)

The [DAGs](https://airflow.apache.org/docs/stable/concepts.html#dags) in this project share a common structure. First, they load data (either via queries to third-party APIs or on-prem JDBC databases) to Google Cloud Storage, in JSON or CSV format. For many data sources (particularly those on-prem), there exist R scripts from our legacy system to do the data extraction. To avoid duplicating this work, we've packaged a number of these scripts in Docker images stored privately on Google Container Registry. We use Airflow's `DockerOperator` to invoke those scripts, which in some cases return CSV and in others newline-delimited JSON (Beam requires newline-delimited). 

Where legacy loading scripts don't exist, we load data via Python scripts [stored in this project](https://github.com/CityofPittsburgh/airflow_scripts/tree/master/airflow_scripts/dags/dependencies/gcs_loaders). Whenever possible, we load data as newline-delimited JSON, which is easier to work with downstream. We use Airflow's template variables for DAG execution date to bucket these files by time (e.g. `gs://pghpa_computronix/businesses/2020/02/2020-02-07_business_licenses.json`). That values are stored retroactively, so if we need to rerun a job from the previous week, the files will still get routed to the appropriate time buckets.

Once files are loaded to Cloud Storage, we run [Apache Beam scripts](https://github.com/CityofPittsburgh/airflow_scripts/tree/master/airflow_scripts/dags/dependencies/dataflow_scripts) via Dataflow, Google's hosted Beam offering. These scripts transform the data where necessary (e.g. changing data types, normalizing addresses) and convert it to [avro](https://avro.apache.org/docs/current/). Avro has several advantages: its fast and compact binary format; its self-describing schemas, which provide type safety; and its status as the preferred format for loading to BigQuery. Avro schemas are defined as JSON, and can include an arbitrary number of additional fields beyond data types, so they're useful for storing human-readable metadata (e.g. data steward, publish frequency) along with schema information. 

The Dataflow scripts validate datums against avro schemas, which we push to [this repository](https://github.com/CityofPittsburgh/data-rivers-schemas/) and copy to a Google Cloud Storage bucket. When creating a DAG for a new data source, you'll first need to create the avro schema, push it to the `data-rivers-schemas` repo, and copy it to the `pghpa_avro_schemas` bucket. Note that geocoding/reverse geocoding attributes, or other enrichments added via BigQuery joins, aren't part of these schemas, as they aren't added until later in the pipeline. Where applicable, we add metadata in the avro schemas (e.g. `"reverse_geocoded": "True"` or `"downstream_enrichments": "["foo", "bar"]`) to flag additional fields that appear in the final BigQuery table.

The Dataflow scripts read the schemas from that bucket. Once any necessary transforms are completed and the data has been converted to avro, we upload it to a separate folder (`avro_output`) in the same bucket that houses the source JSON or CSV. Again, these filenames are time-bucketed using Airflow template variables.

Once the avro files are in Cloud Storage, we move to the next step in the Airflow DAG: using Airflow's `GoogleCloudStoragetoBigQuery` operator to (you guessed it) insert the data into BigQuery. In cases where the data needs to be reverse-geocoded, we then use a function from `airflow_utils` to do so using SQL `GEOGRAPHY` operators and create a new table. (Note: We could simplify the DAGS by creating BigQuery views for the geo-joining portion, but Google BI Engine [doesn't support](https://cloud.google.com/bi-engine/docs/optimized-sql) `GEOGRAPHY` operators, and is very useful for reducing latency in dashboarding) 


## Running locally
As a start, you'll need to [install and configure](https://airflow.apache.org/docs/stable/installation.html) Apache Airflow. Create your
virtual environment by running `pip install -r requirements.txt` and `pip install -r requirements.txt`. Then,
navigate to `/dags/dependences/dataflow_scripts` and run `python setup.py install` to install the local `dataflow_utils`
module. We recommend using [`pyenv`](https://github.com/pyenv/pyenv) and [`pyenv-virtualenv`](https://github.com/pyenv/pyenv-virtualenv) to manage your virtual environments. 


Once you've created your virtual environment and installed Airflow, you can trigger tasks or entire DAGs locally via `airflow run my_dag my_task YYYY-MM-DD` or `airflow trigger_dag my_dag`. To view progress/logging in the Airflow web UI, you'll need to run the processes `airflow webserver` (which shows on port 8080) and `airflow scheduler` in two separate terminal windows. Make sure you have the proper `.env` values available in the shell sessions from which you run those commands, and that you've activated this project's virtualenv in those sessions.

Note that when running DAGs locally that use a `DockerOperator` or `GKEPodOperator` (the GCP flavor of Docker), the jobs will fail if you haven't pulled the relevant images from Google Container Registry to your machine. Example pull command: `docker pull gcr.io/data-rivers/pgh-trash-can-api`

A lot of these images are pretty bulky, so if you're working on a machine with limited disk space, it may make sense to delete them (via the `docker rmi` command) once you've completed your work, then re-pull as needed.

Consult `env.example` for the necessary environment variables (talk to James or Sihan to get these). We recommend installing [`autoenv`](https://github.com/inishchith/autoenv) via homebrew so that your system sources environment variables automatically when you enter the directory.

You'll see that we use the variables `GCLOUD_PROJECT` and `GCS_PREFIX` throughout the scripts. In your local environment, these should be set to `data-rivers-testing` and `pghpa_test`, respectively (this is extremely important). In the production environment (hosted via Cloud Composer), the variables are set to `data-rivers` and `pghpa`. This gives us a testing sandbox for local development while walling off the production environment from code that hasn't yet been merged and deployed from `master`.

## Backfilling data
When you write and deploy a new DAG, there will often be historical data that you'll want to load to BigQuery. The best way I
(James, Nov. 2020) have found to do this is to run a one-off script to download the historical data into a directory named
`/backfill` in a Google Cloud Storage bucket. Then, on your local machine, adjust the relevant DAG file to comment out the
GCS load step from the beginning and hard-code the path to the `/backfill` folder into the arguments passed to the Dataflow step.
From there, run the DAG from your terminal with `airflow trigger_dag your_dag`. That will load the historical data to BigQuery.
Once you've confirmed it's there, you can uncomment the initial GCS step in the DAG.

The backfills will typically be either `SELECT *` statements hitting on-prem databases, or API calls (possibly fired off in a loop
if rate limits pose a challenge for high-volume historical datasets). These backfill scripts will typically be one-offs,
so they don't need to be committed to this repo; if you want to store them for the team for reference, you can put them
[here](https://github.com/CityofPittsburgh/data-rivers-schemas/tree/master/scripts).

## Schema migrations
You'll occasionally want to update the schema for a dataset (adding or subtracting fields). The best way to do this is as follows:
- Update the .avsc file in [data-rivers-schemas](https://github.com/CityofPittsburgh/data-rivers-schemas) with the additional column(s) or redactions
- If necessary, [edit the relevant table schemas](https://cloud.google.com/bigquery/docs/managing-table-schemas#console) via the BigQuery console (do this first in `data-rivers-testing`)
- Refactor the relevant Dataflow file if needed (e.g. change field names, change data types)
- Profit

## Error alerting
Join the "Airflow errors" channel on Microsoft Teams to get alerts when production DAGs fail, including links to the relevant error logs.

## Tests
Write tests for every new Dataflow script. You can execute the entire test suite by running `pytest` from the project root (please do so before making any new pull requests).

CI/CD = work in progress
