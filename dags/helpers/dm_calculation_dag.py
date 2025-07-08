from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from helpers.dm_calculation import (
    init_balances,
    calculate_dm_tables_for_january_2018
)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dm_calculation',
    default_args=default_args,
    description='Расчет витрин данных DM',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['finance', 'dm'],
) as dag:

    start = EmptyOperator(task_id='start')
    
    init_task = PythonOperator(
        task_id='init_balances_20171231',
        python_callable=init_balances,
    )

    calculate_task = PythonOperator(
        task_id='calculate_dm_tables',
        python_callable=calculate_dm_tables_for_january_2018,
    )

    end = EmptyOperator(task_id='end')

    start >> init_task >> calculate_task >> end