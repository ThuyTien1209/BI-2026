import os
import requests
import pandas as pd
from dotenv import load_dotenv
from airflow import DAG
from airflow.decorators import task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.utils.dates import days_ago

load_dotenv()  # Load environment variables from .env file

with DAG(
    dag_id="supabase_to_bigquery",
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
    tags=["supabase", "bigquery", "elt"],
) as dag:

    TABLES = [
        {
            "supabase_table": "customers",           # ← tên bảng trong Supabase
            "bigquery_table": "bi-supplychain.bi_dataset.customers"  # ← BQ table ID
        },
        {
            "supabase_table": "orders",
            "bigquery_table": "bi-supplychain.bi_dataset.orders"
        },
        {
            "supabase_table": "order_items",
            "bigquery_table": "bi-supplychain.bi_dataset.order_items"
        },
        {
            "supabase_table": "products",
            "bigquery_table": "bi-supplychain.bi_dataset.products"
        },
        {
            "supabase_table": "customer_targets",
            "bigquery_table": "bi-supplychain.my_dataset.customer_targets"
        }
    ]

    @task()
    def extract_data_from_supabase(table_name):
        supabase_url = os.getenv("SUPABASE_URL")   # ← Project URL
        supabase_key = os.getenv("SUPABASE_KEY")   # 
        headers = {
            "apikey": supabase_key,
            "Authorization": f"Bearer {supabase_key}",
        }
        response = requests.get(
            f"{supabase_url}/rest/v1/{table_name}",
            headers=headers
        )
        response.raise_for_status()
        extracted_file = f"/tmp/{table_name}_data.csv"
        pd.DataFrame(response.json()).to_csv(extracted_file, index=False)
        return extracted_file

    @task()
    def load_data_to_bigquery(file_path, bigquery_table):
        bq_hook = BigQueryHook(gcp_conn_id="google_cloud_default")
        bq_hook.insert_rows_from_dataframe(
            table=bigquery_table,
            dataframe=pd.read_csv(file_path),
            write_disposition="WRITE_TRUNCATE",  
        )

    for table in TABLES:
        extracted_file = extract_data_from_supabase(table["supabase_table"])
        load_data_to_bigquery(extracted_file, table["bigquery_table"])