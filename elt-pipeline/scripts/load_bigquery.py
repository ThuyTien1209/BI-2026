import pandas as pd
from google.cloud import bigquery
from datetime import datetime, timezone

PROJECT_ID = "bi-project-2026"
DATASET = "raw"

def load_table(df, table_name: str, run_id:str):
    client = bigquery.Client()

    table_id = f"{PROJECT_ID}.{DATASET}.{table_name}"
    df["_extracted_at"] = datetime.now(timezone.utc)
    df["run_id"] = run_id

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        autodetect=True,
    )

    sql_clean = f"DELETE FROM `{table_id}` WHERE run_id = '{run_id}'"
    try:
        client.query(sql_clean).result() 
    except Exception:
        pass 

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    print(f"[LOAD] Appended {len(df)} rows → {table_id}")