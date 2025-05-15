from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import sqlalchemy

def load_csv_to_postgres():
    df = pd.read_csv('/opt/airflow/dags/../data/sales.csv')
    engine = sqlalchemy.create_engine('postgresql+psycopg2://airflow:airflow@postgres/airflow_db')
    df.to_sql('raw_sales', engine, if_exists='replace', index=False, chunksize=1000)

default_args = {
    'start_date': datetime(2023, 1, 1),
    'catchup': False
}

with DAG(
    dag_id='load_sales_csv',
    schedule_interval='@daily',
    default_args=default_args,
    tags=['ingestion'],
) as dag:
    load_task = PythonOperator(
    task_id='load_csv',
    python_callable=load_csv_to_postgres,
    execution_timeout=timedelta(minutes=5),
    )
