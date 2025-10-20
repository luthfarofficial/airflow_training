FROM apache/airflow:2.10.0-python3.11
RUN pip install dbt-bigquery pandas google-cloud-bigquery slack_sdk apache-airflow-providers-slack
