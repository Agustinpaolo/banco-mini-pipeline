"""
etl_transactions.py

Script ETL simple en pandas para:
- Leer data/raw/transactions.csv
- Filtrar transacciones que representan débito (withdrawal o transfer)
- Agrupar por Sender Account ID y calcular total, promedio y cantidad
- Guardar el resultado en data/processed/sender_debits_summary.csv
"""

import pandas as pd
from pathlib import Path

RAW_PATH = Path("data/raw/transactions.csv")
OUTPUT_PATH = Path("data/processed/sender_debits_summary.csv")


def run_etl():
    """
    Ejecuta el pipeline ETL (Extract, Transform, Load).

    Pasos:
    1. Leer el CSV crudo definido en RAW_PATH.
    2. Normalizar la columna "Transaction Type" (strip + lower).
    3. Filtrar los tipos que representan salida de dinero: "withdrawal" y "transfer".
       - Esto considera que en el dataset no existe "debit" explícito.
    4. Asegurar que "Transaction Amount" sea numérico y eliminar filas sin monto.
    5. Agrupar por "Sender Account ID" y calcular:
       - total_debit: suma de montos
       - avg_debit: promedio de montos
       - debit_count: cantidad de transacciones
    6. Guardar el resultado en OUTPUT_PATH (CSV sin índice).

    Resultado:
        Archivo CSV en data/processed/sender_debits_summary.csv con columnas:
        Sender Account ID, total_debit, avg_debit, debit_count
    """
    # 1) EXTRACT: leer el CSV crudo
    df = pd.read_csv(RAW_PATH)

    # Nombres de columnas según tu CSV
    col_customer = "Sender Account ID"
    col_amount = "Transaction Amount"
    col_type = "Transaction Type"

    # Normalizar tipo (eliminar espacios y pasar a minúsculas)
    df[col_type] = df[col_type].astype(str).str.strip().str.lower()

    # Tipos que consideramos como débito (salida de dinero)
    debit_types = ["withdrawal", "transfer"]

    # 2) TRANSFORM: filtrar por tipos de débito
    df_debits = df[df[col_type].isin(debit_types)].copy()

    # Asegurar que el monto sea numérico; valores no convertibles pasan a NaN
    df_debits[col_amount] = pd.to_numeric(df_debits[col_amount], errors="coerce")

    # Eliminar filas sin monto válido
    df_debits = df_debits.dropna(subset=[col_amount])

    # Agrupar por cuenta emisora y calcular métricas
    summary = (
        df_debits
        .groupby(col_customer)[col_amount]
        .agg(
            total_debit="sum",
            avg_debit="mean",
            debit_count="count",
        )
        .reset_index()
    )

    # 3) LOAD: crear carpeta si hace falta y guardar CSV
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    summary.to_csv(OUTPUT_PATH, index=False)

    print(f"ETL completado. Archivo generado en: {OUTPUT_PATH}")


if __name__ == "__main__":
    run_etl()
