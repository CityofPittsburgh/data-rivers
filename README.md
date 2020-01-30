# airflow-scripts
Airflow ETL scripts for Data Rivers project, orchestrated with Google Cloud Composer.

Requirements: Python 3.7.x

## Running locally
Trigger tasks or entire dags locally via `airflow run my_dag my_task YYYY-MM-DD` or `airflow trigger_dag my_dag`. To view progress/logging in the Airflow web UI, you need to run the processes `airflow webserver` (which runs on port 8080) and `airflow scheduler`. Make sure you have the proper `.env` values available in the shell sessions from which you run those commands.

## Tests
Write tests for every new Dataflow script. You can execute the entire test suite by running `pytest` from the project root.
