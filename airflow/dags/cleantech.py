import datetime
import pendulum

from airflow import DAG
from airflow.models import Variable
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.operators.python import PythonOperator
from function.etl import update_nytimes_airbyte_source
from function.etl import arxiv_extract_load

with DAG(
        dag_id='cleantech_pipeline',
        schedule_interval='0 6 * * *',
        start_date=pendulum.datetime(2022, 12, 9, tz="UTC"),
        catchup=True,
        dagrun_timeout=datetime.timedelta(minutes=60),
        tags=['cleantech']
) as workflow:
    run_year = "{{ data_interval_end.format('YYYY') }}"
    run_month = "{{ data_interval_end.format('MM') }}"
    run_date = "{{ data_interval_end.format('YYYYMMDD') }}"

    # Airbyte Connection Info
    airbyte_airflow_conn_id = Variable.get("airbyte-conn-id")
    airbyte_google_scholar_sync_id = Variable.get("airbyte-google-scholar-sync-id")
    airbyte_ny_times_sync_id = Variable.get("airbyte-ny-times-sync-id")

    # Databricks Connection Info
    databricks_airflow_conn_id = Variable.get("databricks-conn-id")
    databricks_run_name = Variable.get("databricks-run-name")

    airbyte_task_google_scholar = AirbyteTriggerSyncOperator(
        task_id='airbyte_google_scholar',
        airbyte_conn_id=airbyte_airflow_conn_id,
        connection_id=airbyte_google_scholar_sync_id,
        asynchronous=False,
        timeout=3600,
        wait_seconds=3
    )

    airbyte_task_update_source_ny_times = PythonOperator(
        task_id="update_airbyte_source_ny_times",
        python_callable=update_nytimes_airbyte_source,
        op_kwargs={
            "run_year": run_year,
            "run_month": run_month
        }
    )

    airbyte_task_ny_times = AirbyteTriggerSyncOperator(
        task_id='airbyte_ny_times',
        airbyte_conn_id=airbyte_airflow_conn_id,
        connection_id=airbyte_ny_times_sync_id,
        asynchronous=False,
        timeout=3600,
        wait_seconds=3
    )

    python_task_extract_arxiv = PythonOperator(
        task_id="custom_arxiv",
        python_callable=arxiv_extract_load
    )

    # Job loads bronze, silver, and gold layers
    databricks_task = DatabricksRunNowOperator(
        task_id='databricks_cleantech',
        job_name=databricks_run_name,
        databricks_conn_id=databricks_airflow_conn_id,
        notebook_params={
            'run_date': run_date
        }
    )

    # Cleantech DAG
    airbyte_task_google_scholar, \
    python_task_extract_arxiv, \
    airbyte_task_update_source_ny_times >> airbyte_task_ny_times \
    >> databricks_task
