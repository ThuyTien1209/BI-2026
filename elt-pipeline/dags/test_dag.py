from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import sys

sys.path.insert(0, "/opt/airflow/scripts")

from extract_supabase import extract
from load_bigquery import load
from google.cloud import bigquery

def extract_task():
    data = extract()
    return data

def load_task(**context):
    data = context["ti"].xcom_pull(task_ids="extract")
    load(data)

def transform_task():
    client = bigquery.Client()
    sql_dir = "/opt/airflow/sql"

    with open(os.path.join(sql_dir, "dim_customers.sql")) as f:
        sql = f.read()
    job = client.query(sql)
    job.result()
    print("Transform dim_customers done")

with DAG(
    dag_id="test_customer",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    t1 = PythonOperator(
        task_id="extract",
        python_callable=extract_task,
    )

    t2 = PythonOperator(
        task_id="load",
        python_callable=load_task,
    )

    t3 = PythonOperator(
        task_id="transform",
        python_callable=transform_task,
    )

    t1 >> t2 >> t3