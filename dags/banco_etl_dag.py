"""
Orquesta un ETL de transacciones bancarias con Airflow.

Lee un CSV crudo, ejecuta una transformación externa en Python y genera un
archivo agregado por 'Sender Account ID'. Mantiene separada la orquestación
(Airflow) de la lógica de negocio.
"""


from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from etl_transactions import etl_transactions


with DAG(
    dag_id="banco_transactions_etl",
    description=(
        "Pipeline ETL que procesa transacciones bancarias: "
        "lectura de CSV, limpieza básica, agregación de montos y generación de un resumen."
    ),
    start_date=datetime(2025, 1, 1),
    schedule=None,  # Se ejecuta manualmente; ideal para desarrollo/demostración.
    catchup=False,
    tags=["banco", "etl"],
):

    etl_task = PythonOperator(
        task_id="run_etl_transactions",
        python_callable=etl_transactions,
        doc="""Ejecuta la función `etl_transactions` del módulo `etl_transactions`.

    Esta tarea:
    - Lee el dataset de transacciones crudo.
    - Aplica la lógica de transformación y agregación.
    - Genera un CSV procesado con métricas por cliente.

    La función no retorna valores; escribe el archivo de salida en
    `/opt/airflow/data/processed`.
    """,
    )
