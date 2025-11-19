# banco-mini-pipeline

Mini proyecto de práctica para trabajar conceptos de datos con **Python**, **Spark** y **Apache Airflow**, usando un dataset de transacciones bancarias ficticias.

La idea es simular un escenario simple de banco:

- Tomar un CSV de transacciones.
- Hacer un ETL básico (lectura, limpieza y agregaciones) con Python.
- Explorar transformaciones con PySpark.
- Orquestar el flujo con Airflow en Docker.

---

## Objetivos del proyecto

- Practicar lectura y procesamiento de datos con **Python**.
- Practicar transformaciones y agregaciones con **PySpark**.
- Dar los primeros pasos en **orquestación de ETL** con **Apache Airflow**.
- Tener un proyecto concreto para mostrar en CV / LinkedIn.

---

## Tecnologías

- Python 3.x
- PySpark (modo local)
- Apache Airflow + Docker Compose
- Git / GitHub

---

## Estructura del proyecto

```text
banco-mini-pipeline/
├── data/
│   ├── raw/
│   │   └── transactions.csv           # Dataset original de transacciones bancarias ficticias
│   └── processed/
│       └── sender_debits_summary.csv  # Salida del ETL en Python
├── etl_transactions.py                # ETL básico en Python
├── spark_transform.py                 # Transformaciones de ejemplo con PySpark
├── airflow/
│   ├── docker-compose.yaml            # Stack de Airflow en Docker
│   ├── dags/
│   │   ├── banco_transactions_etl_dag.py  # DAG que corre un ETL simple sobre el CSV
│   │   └── hello_banco_dag.py            # DAG de prueba (“hola mundo”)
│   ├── logs/                          # Logs generados por Airflow (ignorados en Git)
│   └── plugins/
└── README.md
