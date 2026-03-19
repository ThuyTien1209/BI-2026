import pandas as pd
from google.cloud import bigquery

PROJECT_ID = "bi-project-2026"
DATASET = "raw"

def load(data):

    client = bigquery.Client()

    for table, df in data.items():
        table_id = f"{PROJECT_ID}.{DATASET}.{table}"
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND"
        )

        job = client.load_table_from_dataframe(
            df,
            table_id,
            job_config=job_config
        )
        print(f"Loaded {table}")