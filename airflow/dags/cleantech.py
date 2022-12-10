import datetime
import pendulum

from airflow import DAG
from airflow.models import Variable
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.operators.python import PythonOperator
from function.etl import update_nytimes_airbyte_source
from function.etl import arxiv_extract_load

with DAG(
        dag_id='cleantech_pipeline',
        schedule_interval='0 6 * * *',
        start_date=pendulum.datetime(2022, 12, 1, tz="UTC"),
        catchup=False,
        dagrun_timeout=datetime.timedelta(minutes=60),
        tags=['cleantech']
) as workflow:
    run_year = "{{ data_interval_end.format('YYYY') }}"
    run_month = "{{ data_interval_end.format('MM') }}"

    # Airbyte
    airbyte_airflow_conn_id = Variable.get("airbyte-conn-id")
    airbyte_google_scholar_sync_id = Variable.get("airbyte-google-scholar-sync-id")
    airbyte_ny_times_sync_id = Variable.get("airbyte-ny-times-sync-id")

    # Databricks
    # databricks_airflow_conn = Variable.get("ajp_databricks_connection")

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

    # # ABS tasks - dynamically created from airbyte_abs_endpoints dictionary
    # airbyte_abs_tasks = [
    #     AirbyteTriggerSyncOperator(
    #         task_id=f'airbyte_abs_{next_key}',
    #         airbyte_conn_id=airbyte_airflow_conn_id,
    #         connection_id=next_val,
    #         asynchronous=False,
    #         timeout=3600,
    #         wait_seconds=3
    #     ) for next_key, next_val in airbyte_abs_endpoints.items()
    # ]
    #
    # # Task setup for Databricks - ABS. Just one per layer needed.
    # databricks_task_abs_bronze = DatabricksRunNowOperator(
    #     task_id='databricks_abs_bronze',
    #     job_name=databricks_abs_bronze_job_name,
    #     databricks_conn_id=databricks_airflow_conn
    # )
    #
    # databricks_task_abs_silver = DatabricksRunNowOperator(
    #     task_id='databricks_abs_silver',
    #     job_name=databricks_abs_silver_job_name,
    #     databricks_conn_id=databricks_airflow_conn
    # )
    #
    # # Task setup for Databricks - Domain. Just one per layer needed.
    # databricks_task_domain_bronze = DatabricksRunNowOperator(
    #     task_id='databricks_domain_bronze',
    #     job_name=databricks_domain_bronze_job_name,
    #     databricks_conn_id=databricks_airflow_conn
    # )
    #
    # databricks_task_domain_silver = DatabricksRunNowOperator(
    #     task_id='databricks_domain_silver',
    #     job_name=databricks_domain_silver_job_name,
    #     databricks_conn_id=databricks_airflow_conn
    # )
    #
    # # Final combined dataset
    # databricks_task_combined_gold = DatabricksRunNowOperator(
    #     task_id='databricks_combined_gold',
    #     job_name=databricks_combined_gold_job_name,
    #     databricks_conn_id=databricks_airflow_conn
    # )

    # endregion Tasks

    # region DAG
    airbyte_task_google_scholar, \
    python_task_extract_arxiv, \
    airbyte_task_update_source_ny_times >> airbyte_task_ny_times
    # endregion DAG