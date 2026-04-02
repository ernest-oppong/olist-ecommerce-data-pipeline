import os
import pandas as pd

RAW_DIR = "data/raw"

FILES = [
    "olist_orders_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_customers_dataset.csv",
    "olist_products_dataset.csv",
    "product_category_name_translation.csv",
]

def extract_data():
    dataframes = {}

    for file_name in FILES:
        file_path = os.path.join(RAW_DIR, file_name)

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Missing file: {file_path}")

        df = pd.read_csv(file_path)
        dataframes[file_name] = df
        print(f"Loaded {file_name} -> shape: {df.shape}")

    print("All raw files extracted successfully.")
    return dataframes

if __name__ == "__main__":
    extract_data()