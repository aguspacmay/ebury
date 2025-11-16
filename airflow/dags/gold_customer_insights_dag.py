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

gold_facts_transactions = Dataset('postgres://postgres/analytics/ebury_gold/facts_transactions')

with DAG(
    'gold_customer_insights',
    default_args=default_args,
    description='customer - Insights by transaction',
    schedule=[gold_facts_transactions], 
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['dbt', 'gold', 'customer_insights'],
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
    
    dbt_run_customer_insights = BashOperator(
        task_id='dbt_run_customer_insights_by_transactions',
        bash_command=f'cd {DBT_DIR} && dbt run -s customer_insights_by_transactions',
    )

    dbt_run_customer_totals = BashOperator(
        task_id='dbt_run_customer_totals_by_month',
        bash_command=f'cd {DBT_DIR} && dbt run -s customer_totals_by_month',
    )
    
    # end
    end = EmptyOperator(
        task_id='end',
    )
    
    # Define el orden de ejecución
    start >> dbt_debug >> dbt_deps >> [dbt_run_customer_insights, dbt_run_customer_totals] >> end
