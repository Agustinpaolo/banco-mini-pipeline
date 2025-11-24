"""
etl_transactions.py

Módulo ETL en pandas para procesar transacciones bancarias ficticias.

Este script:
- Lee data/raw/transactions.csv
- Filtra transacciones que representan débito (tipos: withdrawal, transfer)
- Normaliza y valida columnas clave del dataset
- Agrupa por Sender Account ID y calcula total, promedio y cantidad de débitos
- Guarda el resultado en data/processed/sender_debits_summary.csv

Está pensado como un paso de ETL sencillo y reproducible dentro del pipeline
banco-mini-pipeline.
"""

import logging
from pathlib import Path

import pandas as pd

RAW_PATH = Path("data/raw/transactions.csv")
OUTPUT_PATH = Path("data/processed/sender_debits_summary.csv")


def run_etl() -> None:
    """
    Ejecuta el pipeline ETL (Extract, Transform, Load) sobre el CSV de transacciones.

    Flujo general:
        1. Verifica la existencia del archivo RAW_PATH.
        2. Lee el CSV crudo ubicado en RAW_PATH.
        3. Valida que existan las columnas esperadas del dataset bancario.
        4. Normaliza la columna "Transaction Type" (strip + lower).
        5. Filtra las transacciones que representan salida de dinero:
           "withdrawal" y "transfer".
        6. Convierte "Transaction Amount" a numérico y descarta filas sin monto válido.
        7. Calcula el valor absoluto del monto en una columna auxiliar "amount_abs".
        8. Agrupa por "Sender Account ID" y calcula:
           - total_debit: suma de montos
           - avg_debit: promedio de montos
           - debit_count: cantidad de transacciones de débito
        9. Crea la carpeta de salida si no existe y guarda el resultado en OUTPUT_PATH
           como CSV sin índice.

    Efectos:
        - Genera (o sobrescribe) el archivo:
          data/processed/sender_debits_summary.csv

    Raises:
        FileNotFoundError: Si el archivo definido en RAW_PATH no existe.
        ValueError: Si faltan columnas obligatorias en el CSV de entrada.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    if not RAW_PATH.is_file():
        raise FileNotFoundError(f"No se encontró el archivo CSV en {RAW_PATH}")

    logging.info("Leyendo CSV desde %s", RAW_PATH)
    df = pd.read_csv(RAW_PATH)

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
        raise ValueError(f"Faltan columnas en el CSV: {missing}")

    col_customer = "Sender Account ID"
    col_amount = "Transaction Amount"
    col_type = "Transaction Type"

    # Normalizar tipo de transacción (eliminar espacios y pasar a minúsculas)
    df[col_type] = df[col_type].astype(str).str.strip().str.lower()

    # Tipos que consideramos como débito (salida de dinero)
    debit_types = ["withdrawal", "transfer"]
    df_debits = df[df[col_type].isin(debit_types)].copy()

    # Asegurar que el monto sea numérico; valores no convertibles pasan a NaN
    df_debits[col_amount] = pd.to_numeric(df_debits[col_amount], errors="coerce")

    # Eliminar filas sin monto válido
    df_debits = df_debits.dropna(subset=[col_amount])

    # Usar valor absoluto de los montos (defensivo ante posibles signos)
    df_debits["amount_abs"] = df_debits[col_amount].abs()

    # Agrupar por cuenta emisora y calcular métricas
    summary = (
        df_debits
        .groupby(col_customer)["amount_abs"]
        .agg(
            total_debit="sum",
            avg_debit="mean",
            debit_count="count",
        )
        .reset_index()
    )

    # Crear carpeta de salida si hace falta y guardar CSV
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    summary.to_csv(OUTPUT_PATH, index=False)

    logging.info("ETL completado. Archivo generado en: %s", OUTPUT_PATH.resolve())


if __name__ == "__main__":
    run_etl()
