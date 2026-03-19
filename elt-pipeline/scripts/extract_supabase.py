import os
from dotenv import load_dotenv
from supabase import create_client
import pandas as pd

load_dotenv("/home/tien/elt-pipeline/.env")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

TABLES = ["customer", "products", "orders", "target_orders", "order_lines"]  

def extract():
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    data = {}
    for table in TABLES:
        response = supabase.table(table).select("*").execute()
        df = pd.DataFrame(response.data)
        data[table] = df
        print(f"Extracted {table}: {len(df)} rows")
    return data
