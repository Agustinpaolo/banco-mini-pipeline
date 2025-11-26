"""
Transforma datos de transacciones bancarias usando PySpark.

Este script se ejecuta de forma independiente (no desde Airflow):
- Lee el archivo CSV de transacciones bancarias ficticias desde data/raw/transactions.csv
  utilizando una SparkSession.
- Normaliza la columna "Transaction Type" (minúsculas, sin espacios laterales).
- Filtra transacciones que representan débito (tipos: "withdrawal", "transfer").
- Convierte "Transaction Amount" a tipo numérico (float/double) y descarta montos no válidos.
- Calcula métricas agregadas por "Sender Account ID":
    - total_debit: suma del monto absoluto
    - avg_debit: promedio del monto absoluto
    - debit_count: cantidad de transacciones de débito
- Exporta el resultado a un CSV en data/processed/sender_debits_summary_spark.csv.

La idea es replicar, con PySpark, la misma lógica de negocio implementada en el ETL con pandas
(etl_transactions.py), pero usando un motor distribuido.
"""

import logging
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    abs as spark_abs,
    col,
    lower,
    trim,
    sum as spark_sum,
    mean as spark_mean,
    count as spark_count,
)

BASE_DIR = Path(__file__).parent.parent.resolve()
RAW_PATH = BASE_DIR / "data" / "raw" / "transactions.csv"
OUTPUT_PATH = BASE_DIR / "data" / "processed" / "sender_debits_summary_spark.csv"


def run_spark_etl() -> None:
    """
    Ejecuta el pipeline ETL usando PySpark sobre el CSV de transacciones.

    Flujo general:
        1. Crear una SparkSession local.
        2. Leer el archivo CSV ubicado en RAW_PATH con inferencia de esquema.
        3. Validar la existencia de las columnas clave del dataset bancario.
        4. Normalizar la columna "Transaction Type" (strip + lower).
        5. Filtrar transacciones de débito (tipos "withdrawal" y "transfer").
        6. Convertir "Transaction Amount" a tipo numérico (float) y descartar filas sin monto válido.
        7. Crear una columna "amount_abs" con el valor absoluto del monto.
        8. Agrupar por "Sender Account ID" y calcular:
            - total_debit: suma de amount_abs
            - avg_debit: promedio de amount_abs
            - debit_count: cantidad de transacciones de débito
        9. Crear la carpeta de salida si no existe y guardar el resultado como CSV en OUTPUT_PATH.

    Notas:
        - Para este proyecto, el resultado se escribe como un único CSV usando toPandas()
          porque el volumen de datos es pequeño. En un escenario real de big data,
          lo más habitual sería usar el writer de Spark (df.write.csv(...)) y trabajar
          con salidas particionadas en lugar de convertir el resultado a pandas.

    Efectos:
        - Genera (o sobrescribe) el archivo:
          data/processed/sender_debits_summary_spark.csv

    Raises:
        ValueError: Si faltan columnas obligatorias en el CSV de entrada.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    logging.info("Creando SparkSession para job Spark ETL")
    spark = (
        SparkSession.builder.appName("MiniBankPipelineSpark")
        .getOrCreate()
    )

    try:
        logging.info("Leyendo CSV con Spark desde %s", str(RAW_PATH))
        df = spark.read.csv(
                str(RAW_PATH),
                header=True,
                inferSchema=True,
        )

        # Columnas esperadas según el esquema del dataset
        expected_cols = {
            "Transaction ID",
            "Sender Account ID",
            "Receiver Account ID",
            "Transaction Amount",
            "Transaction Type",
            "Timestamp",
            "Transaction Status",
            "Fraud Flag",
            "Geolocation (Latitude/Longitude)",
            "Device Used",
            "Network Slice ID",
            "Latency (ms)",
            "Slice Bandwidth (Mbps)",
            "PIN Code",
        }
        missing = expected_cols - set(df.columns)
        if missing:
            raise ValueError(f"Faltan columnas en el CSV de entrada: {missing}")

        col_customer = "Sender Account ID"
        col_amount = "Transaction Amount"
        col_type = "Transaction Type"

        # Normalizar tipo: eliminar espacios y convertir a minúsculas
        df = df.withColumn(col_type, lower(trim(col(col_type))))

        # Tipos que consideramos débito (coherente con el ETL en pandas)
        debit_types = ["withdrawal", "transfer"]
        df_debits = df.filter(col(col_type).isin(debit_types))

        # Convertir monto a float y descartar filas sin monto válido
        df_debits = df_debits.withColumn(col_amount, col(col_amount).cast("double"))
        df_debits = df_debits.filter(col(col_amount).isNotNull())

        # Valor absoluto del monto
        df_debits = df_debits.withColumn("amount_abs", spark_abs(col(col_amount)))

        # Agrupar por cuenta emisora y calcular métricas agregadas
        summary: DataFrame = (
            df_debits.groupBy(col_customer)
            .agg(
                spark_sum("amount_abs").alias("total_debit"),
                spark_mean("amount_abs").alias("avg_debit"),
                spark_count("amount_abs").alias("debit_count"),
            )
        )

        # Asegurar carpeta de salida
        (OUTPUT_PATH.parent).mkdir(parents=True, exist_ok=True)

        # Para este proyecto, escribimos un único CSV usando pandas
        logging.info("Convirtiendo resultado de Spark a pandas y guardando CSV")
        summary.toPandas().to_csv(str(OUTPUT_PATH), index=False)

        logging.info(
            "Spark ETL completado. Archivo generado en: %s",
            Path(OUTPUT_PATH).resolve(),
        )
    finally:
        logging.info("Deteniendo SparkSession")
        spark.stop()


if __name__ == "__main__":
    run_spark_etl()
