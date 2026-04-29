#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

# Load database credentials from environment variables
USER = os.getenv('DB_USER')
PASS = os.getenv('DB_PASSWORD')
ADRESS = os.getenv('DB_ADRESS')
PORT = os.getenv('DB_PORT')
DB = os.getenv("DB")


def load_raw_data(path):
    """Load raw CSV data from the given file path."""
    print(f"Loading data from: {path}")
    try:
        df = pd.read_csv(path, encoding='ISO-8859-1')
        print(f"Successfully loaded {len(df)} rows.")
        return df
    except FileNotFoundError:
        print('Error: This file does not exist.')
    except pd.errors.EmptyDataError:
        print('Error: This file does not contain any data.')
    except Exception as e:
        print(f'Error! {e}')
    return None


def clean_quantity(df):
    """Remove rows with non-positive Quantity or UnitPrice and save them to rejected_data.csv."""
    print("Validating Quantity and UnitPrice values...")

    mask_invalid = (df['Quantity'] <= 0) | (df['UnitPrice'] <= 0)
    invalid_data = df[mask_invalid]

    if not invalid_data.empty:
        file_path = 'rejected_data.csv'

        if os.path.exists(file_path):
            # Merge with existing rejected records, avoid duplicates
            existing_rejected = pd.read_csv(file_path)
            combined = pd.concat([existing_rejected, invalid_data]).drop_duplicates(subset=['InvoiceNo', 'StockCode'])
            combined.to_csv(file_path, index=False)
            print(f"Rejected records appended to existing {file_path}.")
        else:
            # Create a new rejected_data file
            invalid_data.to_csv('rejected_data.csv', index=False)
            print(f"Rejected records saved to new file: {file_path}.")

        print(f"Warning: {len(invalid_data)} invalid rows detected and removed.")

    valid_data = df[~mask_invalid]
    print(f"Rows remaining after validation: {len(valid_data)}")
    return valid_data


def clean_data(df):
    """Fill missing values — 0 for numeric columns, 'Unknown' for text columns."""
    print("Checking for missing values...")
    missing_data = df.isna().sum()
    missing_data = missing_data[missing_data > 0]

    if missing_data.empty:
        print('Data integrity check passed: No missing values found.')
        return df

    print(f'Found integrity errors in {len(missing_data)} column(s): {list(missing_data.index)}')

    for col in missing_data.index:
        if df[col].dtype in ['float64', 'int64', 'int32']:
            df[col] = df[col].fillna(0)
            print(f"  Column '{col}': missing values filled with 0.")
        else:
            df[col] = df[col].fillna('Unknown')
            print(f"  Column '{col}': missing values filled with 'Unknown'.")

    return df


def save_to_db(df_customers, df_products, df_orders):
    """Save transformed DataFrames to PostgreSQL database."""
    print(f"Connecting to database as: {USER}")
    db_url = f'postgresql://{USER}:{PASS}@{ADRESS}:{PORT}/{DB}'
    engine = create_engine(db_url)

    try:
        print(f"Saving {len(df_customers)} customer records...")
        df_customers.to_sql('customers', engine, if_exists='append', index=False)

        print(f"Saving {len(df_products)} product records...")
        df_products.to_sql('products', engine, if_exists='append', index=False)

        print(f"Saving {len(df_orders)} order records...")
        df_orders.to_sql('orders', engine, if_exists='append', index=False)

        print("All data successfully saved to the database!")
        engine.dispose()
        print("Database connection closed.")
    except Exception as e:
        print(f"Error while saving to database: {e}")


def run():
    """Main ETL pipeline: Extract → Transform → Load."""
    print("=== ETL Pipeline started ===")

    df = load_raw_data('data.csv')

    if df is None:
        print("Pipeline aborted: no data to process.")
        return

    df = clean_quantity(df)
    df = clean_data(df)

    # Split the cleaned data into dimension and fact tables
    df_customers = df[['CustomerID', 'Country']].drop_duplicates()
    df_products = df[['StockCode', 'Description', 'UnitPrice']].drop_duplicates(subset=['StockCode'])
    df_orders = df[['InvoiceNo', 'InvoiceDate', 'CustomerID', 'StockCode', 'Quantity']]

    save_to_db(df_customers, df_products, df_orders)

    print("=== ETL Pipeline finished ===")


if __name__ == '__main__':
    run()
