# from airflow import DAG
# from airflow.decorators import task
# from datetime import datetime

# import sys
# sys.path.insert(0, "/opt/airflow/scripts")
# from extract_supabase import extract
# from load_bigquery import load

# from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


# with DAG(
#     dag_id="supabase_to_bigquery_elt",
#     start_date=datetime(2024,1,1),
#     schedule="@daily",
#     catchup=False
# ) as dag:

#     @task
#     def extract_task():
#         return extract()

#     @task
#     def load_task(df):
#         load(df)

#     extract_data = extract_task()
#     load_data = load_task(extract_data)
#     transform_task = BigQueryInsertJobOperator(

#         task_id="transform_orders",

#         configuration={

#             "query": {

#                 "query": open("/opt/airflow/sql/transform_orders.sql").read(),

#                 "useLegacySql": False,

#             }

#         },

#     )


#     extract_data >> load_data >> transform_task