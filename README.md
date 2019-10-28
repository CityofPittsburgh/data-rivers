# airflow-scripts
Airflow ETL scripts for Data Rivers project, orchestrated with Google Cloud Composer.

Requirements: Python 2.7.x

## Configuration
See `airflow_cfg.example` for formatting of your `airflow.cfg` file, which should be housed in the directory root.

## Running locally
Trigger tasks or entire dags locally via `airflow run my_task my_dag YESTERDAY` or `airflow trigger_dag my_dag`. To view progress/logging in the Airflow web UI, you need to run the processes `airflow webserver` (which runs on port 8080) and `airflow scheduler`. Make sure you have the proper `.env` values available in the shell sessions from which you run those commands.
