from airflow.providers.google.cloud.operators.bigquery import BigQueryGetDataOperator
from airflow import DAG
from datetime import datetime


PROJECT_ID = "sliide-grip-prod"
DATASET_NAME = "dbt_production"
TABLE_NAME = "event_config"

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