import os
import pandas as pd
from sqlalchemy import create_engine, text

PROCESSED_DIR = "data/processed"

DB_USER = "airflow"
DB_PASSWORD = "airflow"
DB_HOST = "postgres"
DB_PORT = "5432"
DB_NAME = "airflow"

def load_csv(file_name: str) -> pd.DataFrame:
    file_path = os.path.join(PROCESSED_DIR, file_name)
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Missing file: {file_path}")
    return pd.read_csv(file_path)

def load_data():
    dim_customers = load_csv("dim_customers.csv")
    dim_products = load_csv("dim_products.csv")
    dim_payments = load_csv("dim_payments.csv")
    fact_orders = load_csv("fact_orders.csv")

    engine = create_engine(
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )

    create_tables_sql = """
    CREATE TABLE IF NOT EXISTS dim_customers (
        customer_id VARCHAR(50) PRIMARY KEY,
        customer_unique_id VARCHAR(50),
        customer_city VARCHAR(100),
        customer_state VARCHAR(10)
    );

    CREATE TABLE IF NOT EXISTS dim_products (
        product_id VARCHAR(50) PRIMARY KEY,
        product_category_name VARCHAR(100),
        product_category_name_english VARCHAR(100)
    );

    CREATE TABLE IF NOT EXISTS dim_payments (
        order_id VARCHAR(50),
        payment_sequential INT,
        payment_type VARCHAR(50),
        payment_installments INT,
        payment_value NUMERIC(12,2)
    );

    CREATE TABLE IF NOT EXISTS fact_orders (
        order_id VARCHAR(50),
        customer_id VARCHAR(50),
        order_status VARCHAR(50),
        order_purchase_timestamp TIMESTAMP,
        order_approved_at TIMESTAMP,
        order_delivered_carrier_date TIMESTAMP,
        order_delivered_customer_date TIMESTAMP,
        order_estimated_delivery_date TIMESTAMP,
        order_item_id INT,
        product_id VARCHAR(50),
        seller_id VARCHAR(50),
        shipping_limit_date TIMESTAMP,
        price NUMERIC(12,2),
        freight_value NUMERIC(12,2),
        delivery_time_days INT,
        estimated_vs_actual_days INT,
        total_order_value NUMERIC(12,2),
        purchase_year INT,
        purchase_month INT
    );
    """

    with engine.begin() as conn:
        conn.execute(text(create_tables_sql))

        # Clear old data before reload
        conn.execute(text("DELETE FROM fact_orders;"))
        conn.execute(text("DELETE FROM dim_payments;"))
        conn.execute(text("DELETE FROM dim_products;"))
        conn.execute(text("DELETE FROM dim_customers;"))

    dim_customers.to_sql("dim_customers", engine, if_exists="append", index=False)
    dim_products.to_sql("dim_products", engine, if_exists="append", index=False)
    dim_payments.to_sql("dim_payments", engine, if_exists="append", index=False)
    fact_orders.to_sql("fact_orders", engine, if_exists="append", index=False)

    print("All processed data loaded into PostgreSQL successfully.")

if __name__ == "__main__":
    load_data()