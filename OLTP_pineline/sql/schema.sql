
-- DROP TABLE (DEV ONLY)
DROP TABLE IF EXISTS order_lines;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS targets_orders;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS customers;


-- CUSTOMERS

CREATE TABLE customers (
    customer_id TEXT PRIMARY KEY,
    customer_name TEXT,
    city TEXT
);

-- PRODUCTS

CREATE TABLE products (
    product_id TEXT PRIMARY KEY,
    product_name TEXT,
    category TEXT,
    price NUMERIC,
    cost NUMERIC
);


-- TARGETS
CREATE TABLE targets_orders (
    customer_id TEXT PRIMARY KEY,
    ontime_target FLOAT,
    infull_target FLOAT,
    otif_target FLOAT,

    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

-- ORDERS

CREATE TABLE orders (
    order_id TEXT PRIMARY KEY,
    customer_id TEXT,
    order_placement_date TIMESTAMP,

    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);


-- ORDER LINES
CREATE TABLE order_lines (
    order_line_id TEXT PRIMARY KEY,
    order_id TEXT,
    product_id TEXT,
    order_qty INT,
    delivery_qty INT,
    agreed_delivery_date TIMESTAMP,
    actual_delivery_date TIMESTAMP,

    FOREIGN KEY (order_id) REFERENCES orders(order_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);