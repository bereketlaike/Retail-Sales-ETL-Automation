from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator



default_args = {
    'owner': 'Bereket Tafesse',
    'start_date': datetime(2025, 1, 1),  # The date when the DAG starts
}

# Define the DAG
dag = DAG(
    'ELT_postgresql',             # DAG ID
    default_args=default_args,     # Default arguments for the DAG
    description='Analytics ELT with PostgreSQL sales data',
    tags=['Data Engineering courses',"Intermediate"],
    catchup= False,              # Do not backfill past runs when DAG is created
    schedule='0 0 * * *'  # Schedule interval (run at midnight every day)
)

# The start point of the DAG, represented by a DummyOperator
start = DummyOperator(
    task_id='start',
    dag=dag
)

# The second task, perform an aggregation of EA year sale data, represented by a BashOperator
store_sales_summary = BashOperator(
    task_id='store_sales_summary',
    bash_command='python /opt/airflow/dags/ETL_postgresql_dag/python_tasks/store_sales_summary.py',
    dag=dag
)

# The third task, perform an aggregation of Nintendo year sales data, represented by a BashOperator
payment_method_analysis = BashOperator(
    task_id='payment_method_analysis',
    bash_command='python /opt/airflow/dags/ETL_postgresql_dag/python_tasks/payment_method_analysis.py',
    dag=dag
)

# The fourth task, perform an aggregation of Ubisoft year sales data, represented by a BashOperator
daily_sales_country_currency = BashOperator(
    task_id='daily_sales_country_currency',
    bash_command='python /opt/airflow/dags/ETL_postgresql_dag/python_tasks/daily_sales_country_currency.py',
    dag=dag
)

# The end point of the DAG, represented by a DummyOperator
stop = DummyOperator(
    task_id='stop',
    dag=dag
)

start >> [store_sales_summary, payment_method_analysis, daily_sales_country_currency] >> stop