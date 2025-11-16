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

bronze_customer_transactions = Dataset('postgres://postgres/analytics/ebury_bronze/customer_transactions')

silver_customer_transactions = Dataset('postgres://postgres/analytics/ebury_silver/customer_transactions')

with DAG(
    'silver_customer_transactions',
    default_args=default_args,
    description='customer_transactions- Bronze to Silver/Error (triggered by dataset)',
    schedule=[bronze_customer_transactions], 
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['dbt', 'silver', 'ingestion', 'dataset-consumer'],
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
    
    # exe validated_transactions (silver)
    dbt_run_validated = BashOperator(
        task_id='dbt_run_validated_transactions',
        bash_command=f'cd {DBT_DIR} && dbt run -s validated_transactions',
        outlets=[silver_customer_transactions]
    )
    
    # exe error_transactions (error)
    dbt_run_errors = BashOperator(
        task_id='dbt_run_error_transactions',
        bash_command=f'cd {DBT_DIR} && dbt run -s error_transactions',
    )
    
    # end
    end = EmptyOperator(
        task_id='end',
    )
    
    # Define el orden de ejecución
    start >> dbt_debug >> dbt_deps >> [dbt_run_validated, dbt_run_errors] >> end
