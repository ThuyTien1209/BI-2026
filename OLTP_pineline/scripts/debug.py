def debug_products(df):
    print("\nDEBUG PRODUCTS")

    print("Duplicate product_id:", df['product_id'].duplicated().sum())
    print("Null values:\n", df.isnull().sum())

    # cost phải < price
    invalid = df[df['cost'] >= df['price']]
    print("Invalid cost >= price:", len(invalid))


def debug_orders(orders, order_lines):
    print("\n DEBUG ORDERS")

    print("Orders duplicate:", orders['order_id'].duplicated().sum())
    print("Order_lines duplicate:", order_lines['order_line_id'].duplicated().sum())

    # FK check
    missing_orders = set(order_lines['order_id']) - set(orders['order_id'])
    print("Missing order_id in orders:", len(missing_orders))


def debug_targets(df):
    print("\n DEBUG TARGETS")

    print("Duplicate customer:", df['customer_id'].duplicated().sum())

    for col in ['ontime_target', 'infull_target', 'otif_target']:
        invalid = df[(df[col] < 0) | (df[col] > 1)]
        print(f"{col} invalid:", len(invalid))


def debug_customers(df):
    print("\n DEBUG CUSTOMERS")

    print("Duplicate ID:", df['customer_id'].duplicated().sum())
    print("Null values:\n", df.isnull().sum())
def debug_fk(order_lines, orders, products):
    print("\n DEBUG FK")
    missing_orders = set(order_lines['order_id']) - set(orders['order_id'])
    missing_products = set(order_lines['product_id']) - set(products['product_id'])

    print(" Missing order_id:", len(missing_orders))
    print(" Missing product_id:", len(missing_products))

    if missing_orders:
        print(list(missing_orders)[:5])

    if missing_products:
        print(list(missing_products)[:5])