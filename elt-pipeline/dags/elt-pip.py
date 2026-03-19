from airflow import DAG
from airflow.decorators import task
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
from airflow.sdk import get_current_context
from datetime import datetime
import logging
import sys

sys.path.insert(0, "/opt/airflow/scripts")
from extract_supabase import extract_table
from load_bigquery import load_table

RAW_TABLES = [
    "customer",
    "products",
    "orders",
    "order_lines",
    "target_orders",
]

DIM_TRANSFORMS = [
    ("dim_customers",      "dim_customers.sql"),
    ("dim_products",       "dim_products.sql"),
    ("dim_date",           "dim_date.sql"),
    ("dim_targets_orders", "dim_targets_orders.sql"),
]

FACT_TRANSFORMS = [
    ("fact_order_lines",      "fact_order_lines.sql"),
    ("fact_orders_aggregate", "fact_orders_aggregate.sql"),
]


def make_bq_operator(task_id: str, sql_file: str) -> BigQueryInsertJobOperator:
    return BigQueryInsertJobOperator(
        task_id=task_id,
        configuration={
            "query": {
                "query": open(f"/opt/airflow/sql/{sql_file}").read(),
                "useLegacySql": False,
            }
        },
    )

# DAG 

with DAG(
    dag_id="supabase_to_bigquery_elt",
    start_date=datetime(2026,3,19),
    schedule="@weekly",
    catchup=False,
    tags=["hkd2026", "elt"],
) as dag:

    # EXTRACT & LOAD

    load_task_groups = []

    for table in RAW_TABLES:

        with TaskGroup(group_id=f"extract_load_{table}") as tg:

            @task(task_id=f"extract_{table}", retries=0)
            def extract_fn(table_name=table):
                context = get_current_context()
                run_id = context["dag_run"].run_id

                logging.info(f"[EXTRACT] Starting  → Supabase.{table_name} | run_id={run_id}")

                df = extract_table(table_name)
                df["run_id"] = run_id
                logging.info(f"[EXTRACT] Completed → Supabase.{table_name} | rows: {len(df)} | run_id={run_id}")
                return df

            @task(task_id=f"load_{table}", retries=0)
            def load_fn(df, table_name=table):
                context = get_current_context()
                run_id = context["dag_run"].run_id

                logging.info(f"[LOAD]    Starting  → hkd_raw.{table_name}")
                load_table(df, table_name, run_id)
                logging.info(f"[LOAD]    Completed → hkd_raw.{table_name} | rows: {len(df)}")

            extracted = extract_fn()
            loaded    = load_fn(extracted)
            extracted >> loaded

        load_task_groups.append(tg)

    # TRANSFORM DIM

    with TaskGroup(group_id="transform_dim") as tg_dim:
        for name, sql_file in DIM_TRANSFORMS:
            make_bq_operator(task_id=name, sql_file=sql_file)

    # TRANSFORM FACT

    with TaskGroup(group_id="transform_fact") as tg_fact:
        fact_ops = []
        for name, sql_file in FACT_TRANSFORMS:
            op = make_bq_operator(task_id=name, sql_file=sql_file)
            fact_ops.append(op)
        fact_ops[0] >> fact_ops[1]


    load_task_groups >> tg_dim >> tg_fact