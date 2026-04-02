from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os

sys.path.append(os.path.abspath("/opt/airflow/scripts"))

from extract import extract_data
from transform import transform_data
from load import load_data

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "retries": 1
}

with DAG(
    dag_id="olist_pipeline",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    description="End-to-end Olist e-commerce ETL pipeline",
    tags=["etl", "postgres", "airflow", "olist"]
) as dag:

    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data
    )

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data
    )

    load_task = PythonOperator(
        task_id="load_data",
        python_callable=load_data
    )

    extract_task >> transform_task >> load_task