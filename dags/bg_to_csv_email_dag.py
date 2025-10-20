from airflow.providers.google.cloud.operators.bigquery import BigQueryGetDataOperator
from airflow import DAG
from datetime import datetime
import pandas as pd
import os
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.hooks.base import BaseHook
from slack_sdk import WebClient

PROJECT_ID = "sliide-grip-prod"
DATASET_NAME = "dbt_production"
TABLE_NAME = "event_config"

CSV_FILE_PATH = os.path.join(os.getcwd(), 'event_config.csv')


def test_slack():
    conn = BaseHook.get_connection("slack_conn_id") # receiving the connection info from metadatabase
    client = WebClient(token=conn.password)
    client.chat_postMessage(
        channel="#airflow-file", 
        text="Slack test message from Airflow!"
)
    
def save_to_csv(**context):
    data = context['task_instance'].xcom_pull(task_ids='get_data')
    df = pd.DataFrame(data)
    # get current working directory, file name
    output_path = os.path.join(os.getcwd(), 'event_config.csv')
    df.to_csv(output_path, index=False)
    print(f"Data saved to {output_path}")

def send_file_to_slack(**context):
    conn=BaseHook.get_connection("slack_conn_id")
    client= WebClient(token=conn.password)
    file_path = os.path.join(os.getcwd(), 'event_config.csv')
    client.files_upload_v2(
    channel="C09LX74LM7B",            
    initial_comment="Here is the latest CSV",
    filename="event_config.csv",        
    file=file_path                   
)


with DAG(
    dag_id="bg_to_csv_email_dag",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:
    get_data = BigQueryGetDataOperator(
        task_id="get_data",
        project_id=PROJECT_ID,
        dataset_id=DATASET_NAME,
        table_id=TABLE_NAME,
        gcp_conn_id="gcp_conn_id",
    )
    save_csv = PythonOperator(
        task_id='save_to_csv',
        python_callable=save_to_csv,
    )

    test_slack_task = PythonOperator(
        task_id="test_slack",
        python_callable=test_slack,
    )

    send_file_to_slack= PythonOperator(
        task_id='send_file_to_slack',
        python_callable=send_file_to_slack,
        
    )

    get_data >> save_csv >> test_slack_task >> send_file_to_slack