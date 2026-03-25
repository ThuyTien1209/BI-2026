import pandas as pd
import numpy as np
import re
import os

np.random.seed(42)
os.makedirs("data/processed", exist_ok=True)

def get_product_key(product_name):
    return re.sub(r'\d+', '', product_name).strip()


# PRICE LOGIC
def get_base_price(product_name):
    if 'Milk' in product_name:
        return np.random.uniform(6, 9)
    elif 'Butter' in product_name:
        return np.random.uniform(9, 13)
    elif 'Ghee' in product_name:
        return np.random.uniform(12, 16)
    elif 'Curd' in product_name:
        return np.random.uniform(6, 9)
    elif 'Biscuits' in product_name:
        return np.random.uniform(5, 9)
    elif 'Tea' in product_name:
        return np.random.uniform(4, 7)

    # MEAT
    elif 'Chicken' in product_name:
        return np.random.uniform(7, 11)
    elif 'Pork' in product_name:
        return np.random.uniform(9, 14)
    elif 'Beef' in product_name:
        return np.random.uniform(12, 18)
    elif 'Meat' in product_name:
        return np.random.uniform(9, 15)

    else:
        return np.random.uniform(5, 10)


def get_size_multiplier(product_name):
    match = re.findall(r'\d+', product_name)

    if not match:
        return 1

    size = int(match[0])

    return {
        50: 0.5,
        100: 0.65,
        150: 0.8,
        250: 1,
        500: 1.6,
        750: 2.2
    }.get(size, 1)


def adjust_price(price):
    mode = np.random.choice(['half', 'decimal'], p=[0.6, 0.4])
    return round(price * 2) / 2 if mode == 'half' else round(price, 1)


def get_margin_range(category):
    return {
        'Food': (0.2, 0.35),
        'Beverages': (0.3, 0.5),
        'Dairy': (0.25, 0.4),
        'Meat': (0.15, 0.3)
    }.get(category, (0.2, 0.4))


# PROCESS PRODUCTS
def process_products(path="data/raw/dim_products.csv"):

    df = pd.read_csv(path)

    # clean
    df = df.drop_duplicates()
    df['product_id'] = df['product_id'].astype(str)
    df['product_name'] = df['product_name'].astype(str)
    df['category'] = df['category'].astype(str)

    df['category'] = df['category'].replace({
        'beverages': 'Beverages'
    })

    

    # generate price & cost
    base_price_map = {}

    def generate_price(row):
        key = get_product_key(row['product_name'])

        if key not in base_price_map:
            base_price_map[key] = get_base_price(key)

        base_price = base_price_map[key]
        multiplier = get_size_multiplier(row['product_name'])

        price = adjust_price(base_price * multiplier)
        return round(price, 2)

    def generate_cost(row):
        low, high = get_margin_range(row['category'])
        margin = np.random.uniform(low, high)

        cost = row['price'] * (1 - margin)
        cost *= np.random.uniform(0.97, 1.03)

        return round(cost, 2)

    df['price'] = df.apply(generate_price, axis=1)
    df['cost'] = df.apply(generate_cost, axis=1)

    # remove duplicate product_id
    df = df.drop_duplicates(subset=['product_id'])

    #  validate
    assert df['price'].isnull().sum() == 0, "Price null "
    assert df['cost'].isnull().sum() == 0, "Cost null "
    assert (df['cost'] < df['price']).all(), "Cost >= Price "

    print(f" Products processed: {len(df)} rows")

    df.to_csv("data/processed/products.csv", index=False)

    return df


# PROCESS ORDERS
def process_orders(path="data/raw/fact_order_lines.csv"):

    df = pd.read_csv(path)

    # datatype
    df['order_id'] = df['order_id'].astype(str)
    df['customer_id'] = df['customer_id'].astype(str)
    df['product_id'] = df['product_id'].astype(str)

    df['order_placement_date'] = pd.to_datetime(df['order_placement_date'])
    df['agreed_delivery_date'] = pd.to_datetime(df['agreed_delivery_date'])
    df['actual_delivery_date'] = pd.to_datetime(df['actual_delivery_date'])

    df['order_qty'] = df['order_qty'].astype(int)
    df['delivery_qty'] = df['delivery_qty'].astype(int)

    df = df.drop_duplicates()

    # logic clean
    df = df[df['delivery_qty'] <= df['order_qty']]
    df = df[df['actual_delivery_date'] >= df['order_placement_date']]
    df = df[df['agreed_delivery_date'] >= df['order_placement_date']]
    df = df.dropna(subset=['order_id', 'customer_id', 'product_id'])
    df = df[df['order_qty'] > 0]
    df = df[df['delivery_qty'] >= 0]

    # mỗi order 1 customer
    invalid_orders = df.groupby('order_id')['customer_id'].nunique()
    invalid_orders = invalid_orders[invalid_orders > 1]
    df = df[~df['order_id'].isin(invalid_orders.index)]

    # drop columns
    df = df.drop(columns=['In Full', 'On Time', 'On Time In Full'], errors='ignore')

    # split bảng
    orders = df[['order_id', 'customer_id', 'order_placement_date']].drop_duplicates()

    order_lines = df.copy().reset_index(drop=True)
    order_lines['order_line_id'] = order_lines.index + 1
    order_lines['order_line_id'] = order_lines['order_line_id'].apply(lambda x: f"OL{x:06d}")

    order_lines = order_lines[[
        'order_line_id',
        'order_id',
        'product_id',
        'order_qty',
        'delivery_qty',
        'agreed_delivery_date',
        'actual_delivery_date'
    ]]

    print(f" Orders: {len(orders)} | Order lines: {len(order_lines)}")
    orders.to_csv("data/processed/orders.csv", index=False)
    order_lines.to_csv("data/processed/order_lines.csv", index=False)

    return orders, order_lines



# PROCESS TARGETS
def process_targets(path="data/raw/dim_targets_orders.csv"):

    df = pd.read_csv(path)

    df = df.rename(columns={
        'ontime_target%': 'ontime_target',
        'infull_target%': 'infull_target',
        'otif_target%': 'otif_target'
    })

    df['customer_id'] = df['customer_id'].astype(str)

    for col in ['ontime_target', 'infull_target', 'otif_target']:
        df[col] = df[col].astype(float) / 100

    df = df.drop_duplicates(subset=['customer_id'])

    print(f" Targets processed: {len(df)} rows")
    df.to_csv("data/processed/targets.csv", index=False)

    return df


# PROCESS CUSTOMERS
def process_customers(path="data/raw/dim_customers.csv"):

    df = pd.read_csv(path)

    df['customer_id'] = df['customer_id'].astype(str)
    df['customer_name'] = df['customer_name'].astype(str)
    df['city'] = df['city'].astype(str)

    df['customer_name'] = df['customer_name'].str.strip()
    df['city'] = df['city'].str.strip().str.title()

    df = df.dropna(subset=['customer_id'])
    df['customer_name'] = df['customer_name'].fillna("Unknown")
    df['city'] = df['city'].fillna("Unknown")

    df = df.drop_duplicates(subset=['customer_id'])

    print(f" Customers processed: {len(df)} rows")
    df.to_csv("data/processed/customers.csv", index=False)

    return df