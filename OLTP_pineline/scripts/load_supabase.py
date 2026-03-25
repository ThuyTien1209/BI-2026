from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

from config.db_config import engine


# RUN SCHEMA
def run_schema():
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    schema_path = os.path.join(BASE_DIR, "sql", "schema.sql")

    with open(schema_path, "r", encoding="utf-8") as f:
        schema_sql = f.read()

    with engine.connect() as conn:
        conn.execute(text(schema_sql))
        conn.commit()

    print("Schema created!")


# LOAD TABLE
def load_table(df, table_name, if_exists='append'):
    try:
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists=if_exists,
            index=False,
            method='multi'
        )
        print(f" Loaded {table_name}: {len(df)} rows")

    except Exception as e:
        print(f" Error loading {table_name}: {e}")
        raise

# LOAD ALL
def load_all(products, customers, targets, orders, order_lines):

    print("\n START LOADING...\n")

    load_table(customers, "customers")
    load_table(products, "products")
    load_table(targets, "targets_orders")
    load_table(orders, "orders")
    load_table(order_lines, "order_lines")

    print("\n DONE LOADING!")