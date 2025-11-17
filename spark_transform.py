from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, sum as spark_sum, mean as spark_mean, count as spark_count
from pathlib import Path

RAW_PATH = "data/raw/transactions.csv"
OUTPUT_PATH = "data/processed/customer_debits_summary_spark.csv"


def run_spark_etl():
    # 1) Crear sesión de Spark
    spark = SparkSession.builder \
        .appName("MiniBankPipelineSpark") \
        .getOrCreate()

    # 2) EXTRACT: leer CSV con Spark
    df = spark.read.csv(RAW_PATH, header=True, inferSchema=True)

    # Columnas reales del CSV
    col_customer = "Sender Account ID"
    col_amount = "Transaction Amount"
    col_type = "Transaction Type"

    # Normalizar tipo a minúsculas
    df = df.withColumn(col_type, lower(col(col_type)))

    # En tu dataset las transacciones equivalentes a DEBIT son "withdrawal"
    debit_value = "withdrawal"

    # Filtrar solo débito
    df_debits = df.filter(col(col_type) == debit_value)

    # Convertir monto a float
    df_debits = df_debits.withColumn(col_amount, col(col_amount).cast("float"))

    # 3) TRANSFORM: agrupar por customer_id
    summary = (
        df_debits
        .groupBy(col_customer)
        .agg(
            spark_sum(col_amount).alias("total_debit"),
            spark_mean(col_amount).alias("avg_debit"),
            spark_count(col_amount).alias("debit_count")
        )
    )

    # Asegurar carpeta
    Path("data/processed").mkdir(parents=True, exist_ok=True)

    # 4) LOAD
    summary.toPandas().to_csv(OUTPUT_PATH, index=False)

    spark.stop()

    print(f"Spark ETL completado. Archivo generado en: {OUTPUT_PATH}")


if __name__ == "__main__":
    run_spark_etl()
