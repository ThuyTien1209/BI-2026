import os
from dotenv import load_dotenv
from supabase import create_client
import pandas as pd

load_dotenv("/home/tien/elt-pipeline/.env")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

def extract_table(table_name: str) -> pd.DataFrame:
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    all_rows = []
    page_size = 1000
    offset    = 0

    while True:
        response = (
            supabase.table(table_name)
            .select("*")
            .range(offset, offset + page_size - 1)
            .execute()
        )

        rows = response.data
        if not rows:
            break

        all_rows.extend(rows)
        print(f"[EXTRACT] {table_name} | offset {offset} → {offset + len(rows) - 1}")
        if len(rows) < page_size:
            break

        offset += page_size

    df = pd.DataFrame(all_rows)
    print(f"[EXTRACT] Completed → {table_name} | total rows: {len(df)}")
    return df
