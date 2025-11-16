from datetime import datetime
from airflow import DAG
from airflow.sdk import Asset as Dataset
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

default_args = {
    'owner': 'airflow',
    'retries': 1,
}

# Directorio de dbt
DBT_DIR = '/opt/airflow/dbt'

silver_customer_transactions = Dataset('postgres://postgres/analytics/ebury_silver/customer_transactions')
gold_facts_transactions = Dataset('postgres://postgres/analytics/ebury_gold/facts_transactions')

with DAG(
    'gold_customer_transactions_star_schema',
    default_args=default_args,
    description='customer_transactions- Gold Star Schema',
    schedule=[silver_customer_transactions], 
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['dbt', 'gold', 'star_schema'],
) as dag:
    
    # start
    start = EmptyOperator(
        task_id='start',
    )
    
    # Verify the connection
    dbt_debug = BashOperator(
        task_id='dbt_debug',
        bash_command=f'cd {DBT_DIR} && dbt debug',
    )
    
    # install dependencies if needed
    dbt_deps = BashOperator(
        task_id='dbt_deps',
        bash_command=f'cd {DBT_DIR} && dbt deps',
    )
    
    # Ejecuta dim_product (gold)
    dbt_run_dim_product = BashOperator(
        task_id='dbt_run_dim_product',
        bash_command=f'cd {DBT_DIR} && dbt run -s dim_product',
    )
    
    # Ejecuta facts_transactions (gold)
    dbt_run_facts = BashOperator(
        task_id='dbt_run_facts_transactions',
        bash_command=f'cd {DBT_DIR} && dbt run -s facts_transactions',
        outlets=[gold_facts_transactions]
    )
    
    # end
    end = EmptyOperator(
        task_id='end',
    )
    
    # Define el orden de ejecución
    start >> dbt_debug >> dbt_deps >> [dbt_run_dim_product, dbt_run_facts] >> end
