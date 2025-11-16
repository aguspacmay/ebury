# Ejemplo conceptual - no ejecutable sin ajustes
from airflow import DAG
from airflow.sdk import Asset as Dataset
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException
import pandas as pd
from datetime import datetime
from sqlalchemy.types import Text

# Define el dataset que representa la tabla bronze
# Formato correcto: postgres://host/database/schema/table
bronze_customer_transactions = Dataset('postgres://postgres/analytics/ebury_bronze/customer_transactions')

def load_incremental_data():
    """
    Loads only new data into the bronze layer table
    """
    
    # Lee el CSV con todas las columnas como texto
    df = pd.read_csv('/opt/airflow/data/customer_transactions.csv', dtype=str)
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    with pg_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("CREATE SCHEMA IF NOT EXISTS ebury_bronze")
        conn.commit()
    
    engine = pg_hook.get_sqlalchemy_engine()
    
    try:
        existing_ids = pd.read_sql(
            "SELECT transaction_id FROM ebury_bronze.customer_transactions",
            con=engine
        )
        
        df_new = df[~df['transaction_id'].isin(existing_ids['transaction_id'])]
        
        print(f"Total records in CSV: {len(df)}")
        print(f"Existing records in DB: {len(existing_ids)}")
        print(f"New records to insert: {len(df_new)}")
        
        if len(df_new) == 0:
            raise AirflowSkipException("No new data to process")
        
        df_to_insert = df_new
        
    except AirflowSkipException:
        print("No new records to insert - skipping task")
        raise
    except Exception as e:
        print(f"Table doesn't exist or error occurred: {e}")
        print(f"Inserting all {len(df)} records")
        df_to_insert = df
    
    dtype_dict = {col: Text for col in df_to_insert.columns}
    
    df_to_insert.to_sql(
        'customer_transactions',
        con=engine,
        schema='ebury_bronze',
        if_exists='append',
        index=False,
        method='multi',
        dtype=dtype_dict 
    )
    
    engine.dispose()
    print(f"Successfully inserted {len(df_to_insert)} new records")

with DAG(
    'bronze_customer_transactions',
    description='Customer transaction csv load (incremental)',
    start_date=datetime(2025, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['bronze', 'dataset-producer']
) as dag:
    
    load_task = PythonOperator(
        task_id='load_new_data',
        python_callable=load_incremental_data,
        outlets=[bronze_customer_transactions]
    )
