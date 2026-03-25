from scripts.transform import (
    process_products,
    process_orders,
    process_targets,
    process_customers
)

from scripts.load_supabase import load_all, run_schema
from scripts.debug import *


def main():

    print(" START OLTP PIPELINE\n")
    # STEP 1: SCHEMA
    print(" Creating schema...")

    # STEP 2: TRANSFORM
    print(" Processing products...")
    df_products = process_products()

    print(" Processing orders...")
    orders, order_lines = process_orders()

    print(" Processing targets...")
    df_targets = process_targets()

    print(" Processing customers...")
    df_customers = process_customers()


    # STEP 3: DEBUG
    print("\n Running debug checks...")

    debug_products(df_products)
    debug_orders(orders, order_lines)
    debug_fk(order_lines, orders, df_products)
    debug_targets(df_targets)
    debug_customers(df_customers)
  
    # STEP 4: LOAD
    load_all(
        products=df_products,
        customers=df_customers,
        targets=df_targets,
        orders=orders,
        order_lines=order_lines
    )

    print("\n PIPELINE DONE!")


if __name__ == "__main__":
    main()