from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def say_hello():
    print("Hola desde Airflow, probando el pipeline del banco :)")


with DAG(
    dag_id="hello_banco_dag",
    description="DAG de prueba para verificar que Airflow funciona",
    start_date=datetime(2025, 1, 1),
    schedule=None,  # antes era schedule_interval=None
    catchup=False,
    tags=["demo", "banco"],
):
    hello_task = PythonOperator(
        task_id="say_hello_task",
        python_callable=say_hello,
    )
