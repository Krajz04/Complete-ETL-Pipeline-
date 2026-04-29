#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

USER = os.getenv('DB_USER')
PASS = os.getenv('DB_PASSWORD') # Zamiast DB_PASSWORDS
ADRESS = os.getenv('DB_ADRESS')
PORT = os.getenv('DB_PORT')
DB = os.getenv("DB")

def load_raw_data(path):
    try:
        df = pd.read_csv(path,encoding='ISO-8859-1')

        return df
    except FileNotFoundError:
        print('This file does not exists.')
    except pd.errors.EmptyDataError:
        print('This file does not contain any data.')
    except Exception as e:
        print(f'Error! {e}')
    return None


def clean_quantity(df):

    mask_invalid = (df['Quantity'] <= 0) | (df['UnitPrice'] <= 0 )
    invalid_data = df[mask_invalid]

    if not invalid_data.empty:
        file_path = 'rejected_data.csv'

        if os.path.exists(file_path):
            existing_rejected = pd.read_csv(file_path)
            combined = pd.concat([existing_rejected, invalid_data]).drop_duplicates(subset=['InvoiceNo','StockCode'])
            combined.to_csv(file_path, index= False)
            print('')
        else:
            invalid_data.to_csv('rejected_data.csv', index=False)
            print('')

        print(f"Uwaga: Wykryto {len(invalid_data)} błędnych wierszy. Zapisuję do rejected_data.csv")

    valid_data = df[~mask_invalid]
    return valid_data


def clean_data(df):
    missing_data = df.isna().sum()
    missing_data = missing_data[missing_data  > 0]

    if missing_data.empty:
        print('Data integrity check passed: No missing values found.')
        return df

    print(f'Found some integrity erros in {len(missing_data)} column(s).')

    for col in missing_data.index:
        if df[col].dtype in ['float64','int64','int32']:
            df[col] = df[col].fillna(0)
        else:
            df[col] = df[col].fillna('Unknown')
    return df


def save_to_db(df_customers, df_products, df_orders):
    print(f"Łączę się jako: {USER}") # TO CI POKAŻE, KOGO UŻYWA SKRYPT
    db_url = f'postgresql://{USER}:{PASS}@{ADRESS}:{PORT}/{DB}'
    engine = create_engine(db_url)

    try:
        df_customers.to_sql('customers', engine, if_exists='append', index=False)
        df_products.to_sql('products', engine, if_exists='append', index=False)
        df_orders.to_sql('orders', engine, if_exists='append', index=False)
        print("Dane pomyślnie wgrane do bazy ecommerce_db!")
        engine.dispose()
    except Exception as e:
        print(f"Wystąpił błąd: {e}")



def run():

    df = load_raw_data('data.csv')

    if df is None:
        return

    df = clean_quantity(df)
    df = clean_data(df)

    df_customers = df[['CustomerID','Country']].drop_duplicates()
    df_products = df[['StockCode', 'Description', 'UnitPrice']].drop_duplicates(subset=['StockCode'])
    df_orders = df[['InvoiceNo', 'InvoiceDate', 'CustomerID', 'StockCode', 'Quantity']]

    save_to_db(df_customers,df_products,df_orders)


if __name__ == '__main__':
    run()


# df.isna().sum()

