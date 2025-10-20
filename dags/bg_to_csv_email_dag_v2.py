from airflow.providers.google.cloud.operators.bigquery import BigQueryGetDataOperator
from airflow.decorators import dag, task
from datetime import datetime
import pandas as pd
import os
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from slack_sdk import WebClient
from airflow.providers.slack.operators.slack import SlackAPIFileOperator

PROJECT_ID = "sliide-grip-prod"
DATASET_NAME = "dbt_production"
TABLE_NAME = "event_config"

CSV_FILE_PATH = os.path.join(os.getcwd(), 'event_config.csv')

@dag(
    dag_id="bg_to_csv_email_dag_v2",
    start_date=datetime(2025, 1, 1),
    schedule="0 * * * *",
    catchup=False,
    params={
        "slack_channel":'C09LX74LM7B'
    },
)
def bg_to_csv_dag():

    get_data = BigQueryGetDataOperator(
    task_id="get_data",
    project_id=PROJECT_ID,
    dataset_id=DATASET_NAME,
    table_id=TABLE_NAME,
    gcp_conn_id="gcp_conn_id",
    )

    @task
    def save_to_csv(**context):
        data = context['task_instance'].xcom_pull(task_ids='get_data')
        df = pd.DataFrame(data)
        # get current working directory, file name
        output_path = CSV_FILE_PATH
        df.to_csv(output_path, index=False)
        print(f"Data saved to {output_path}")

    @task
    def test_slack():
        conn = BaseHook.get_connection("slack_conn_id") # receiving the connection info from metadatabase
        client = WebClient(token=conn.password)
        client.chat_postMessage(
            channel="#airflow-file", 
            text="Slack test message from Airflow DAG settings"
    )
    
    upload_file_from_path = SlackAPIFileOperator(
    task_id="upload_file_from_path",
    slack_conn_id="slack_conn_id",
    channel="{{ params.slack_channel }}",
    filename="event_config.csv",
    initial_comment="Slack test message from Airflow DAG settings",
    )

    get_data >> save_to_csv() >> test_slack() >> upload_file_from_path

bg_to_csv_dag()