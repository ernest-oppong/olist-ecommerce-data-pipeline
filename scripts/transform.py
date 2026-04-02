import os
import pandas as pd

RAW_DIR = "data/raw"
PROCESSED_DIR = "data/processed"

def load_csv(file_name: str) -> pd.DataFrame:
    file_path = os.path.join(RAW_DIR, file_name)
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Missing file: {file_path}")
    return pd.read_csv(file_path)

def transform_data():
    os.makedirs(PROCESSED_DIR, exist_ok=True)

    # Load raw data
    orders = load_csv("olist_orders_dataset.csv")
    order_items = load_csv("olist_order_items_dataset.csv")
    order_payments = load_csv("olist_order_payments_dataset.csv")
    customers = load_csv("olist_customers_dataset.csv")
    products = load_csv("olist_products_dataset.csv")
    category_translation = load_csv("product_category_name_translation.csv")

    # Drop duplicates
    orders = orders.drop_duplicates()
    order_items = order_items.drop_duplicates()
    order_payments = order_payments.drop_duplicates()
    customers = customers.drop_duplicates()
    products = products.drop_duplicates()
    category_translation = category_translation.drop_duplicates()

    # Convert datetime columns in orders
    datetime_columns = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date"
    ]

    for col in datetime_columns:
        orders[col] = pd.to_datetime(orders[col], errors="coerce")

    # -----------------------------
    # Dimension: customers
    # -----------------------------
    dim_customers = customers[[
        "customer_id",
        "customer_unique_id",
        "customer_city",
        "customer_state"
    ]].copy()

    # -----------------------------
    # Dimension: products
    # -----------------------------
    dim_products = products[[
        "product_id",
        "product_category_name"
    ]].copy()

    dim_products = dim_products.merge(
        category_translation,
        on="product_category_name",
        how="left"
    )

    dim_products = dim_products.rename(columns={
        "product_category_name_english": "product_category_name_english"
    })

    # Fill missing translated categories
    dim_products["product_category_name_english"] = dim_products[
        "product_category_name_english"
    ].fillna("unknown")

    dim_products["product_category_name"] = dim_products[
        "product_category_name"
    ].fillna("unknown")

    # -----------------------------
    # Dimension: payments
    # -----------------------------
    dim_payments = order_payments[[
        "order_id",
        "payment_sequential",
        "payment_type",
        "payment_installments",
        "payment_value"
    ]].copy()

    # -----------------------------
    # Fact: orders
    # -----------------------------
    fact_orders = orders.merge(
        order_items,
        on="order_id",
        how="inner"
    )

    # Add delivery metrics
    fact_orders["delivery_time_days"] = (
        fact_orders["order_delivered_customer_date"] - fact_orders["order_purchase_timestamp"]
    ).dt.days

    fact_orders["estimated_vs_actual_days"] = (
        fact_orders["order_estimated_delivery_date"] - fact_orders["order_delivered_customer_date"]
    ).dt.days

    fact_orders["total_order_value"] = (
        fact_orders["price"].fillna(0) + fact_orders["freight_value"].fillna(0)
    )

    fact_orders["purchase_year"] = fact_orders["order_purchase_timestamp"].dt.year
    fact_orders["purchase_month"] = fact_orders["order_purchase_timestamp"].dt.month

    fact_orders = fact_orders[[
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
        "order_item_id",
        "product_id",
        "seller_id",
        "shipping_limit_date",
        "price",
        "freight_value",
        "delivery_time_days",
        "estimated_vs_actual_days",
        "total_order_value",
        "purchase_year",
        "purchase_month"
    ]].copy()

    # Convert shipping_limit_date
    fact_orders["shipping_limit_date"] = pd.to_datetime(
        fact_orders["shipping_limit_date"], errors="coerce"
    )

    # Save processed files
    dim_customers.to_csv(os.path.join(PROCESSED_DIR, "dim_customers.csv"), index=False)
    dim_products.to_csv(os.path.join(PROCESSED_DIR, "dim_products.csv"), index=False)
    dim_payments.to_csv(os.path.join(PROCESSED_DIR, "dim_payments.csv"), index=False)
    fact_orders.to_csv(os.path.join(PROCESSED_DIR, "fact_orders.csv"), index=False)

    print("Transformation complete.")
    print(f"dim_customers: {dim_customers.shape}")
    print(f"dim_products: {dim_products.shape}")
    print(f"dim_payments: {dim_payments.shape}")
    print(f"fact_orders: {fact_orders.shape}")

    return {
        "dim_customers": dim_customers,
        "dim_products": dim_products,
        "dim_payments": dim_payments,
        "fact_orders": fact_orders
    }

if __name__ == "__main__":
    transform_data()