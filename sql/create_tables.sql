-- Drop tables if they already exist
DROP TABLE IF EXISTS fact_orders;
DROP TABLE IF EXISTS dim_payments;
DROP TABLE IF EXISTS dim_products;
DROP TABLE IF EXISTS dim_customers;

-- Customers dimension table
CREATE TABLE dim_customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    customer_unique_id VARCHAR(50),
    customer_zip_code_prefix INTEGER,
    customer_city VARCHAR(100),
    customer_state VARCHAR(10)
);

-- Products dimension table
CREATE TABLE dim_products (
    product_id VARCHAR(50) PRIMARY KEY,
    product_category_name VARCHAR(100),
    product_category_name_english VARCHAR(100),
    product_name_length INTEGER,
    product_description_length INTEGER,
    product_photos_qty INTEGER,
    product_weight_g INTEGER,
    product_length_cm INTEGER,
    product_height_cm INTEGER,
    product_width_cm INTEGER
);

-- Payments dimension table
CREATE TABLE dim_payments (
    order_id VARCHAR(50),
    payment_sequential INTEGER,
    payment_type VARCHAR(50),
    payment_installments INTEGER,
    payment_value NUMERIC(10,2)
);

-- Fact orders table
CREATE TABLE fact_orders (
    order_id VARCHAR(50),
    customer_id VARCHAR(50),
    product_id VARCHAR(50),
    order_status VARCHAR(50),
    order_purchase_timestamp TIMESTAMP,
    purchase_year INTEGER,
    purchase_month INTEGER,
    price NUMERIC(10,2),
    freight_value NUMERIC(10,2),
    total_order_value NUMERIC(10,2)
);