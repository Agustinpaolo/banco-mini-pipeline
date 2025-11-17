import pandas as pd
from pathlib import Path


RAW_PATH = Path("data/raw/transactions.csv")
OUTPUT_PATH = Path("data/processed/sender_debits_summary.csv")


def run_etl():
    # 1) EXTRACT: leer el CSV crudo
    df = pd.read_csv(RAW_PATH)

    # Nombres de columnas según tu CSV
    col_customer = "Sender Account ID"
    col_amount = "Transaction Amount"
    col_type = "Transaction Type"

    # Antes de filtrar, normalizamos a minúsculas para evitar problemas
    df[col_type] = df[col_type].astype(str).str.lower()

    # Si en el dataset las transacciones de débito se llaman "debit"
    debit_value = "debit"

    # 2) TRANSFORM

    # Filtrar solo transacciones de débito del emisor
    df_debits = df[df[col_type] == debit_value].copy()

    # Asegurar que el monto sea numérico
    df_debits[col_amount] = pd.to_numeric(df_debits[col_amount], errors="coerce")

    # Eliminar filas con monto nulo
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

    # 3) LOAD: guardar el resultado procesado
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    summary.to_csv(OUTPUT_PATH, index=False)

    print(f"ETL completado. Archivo generado en: {OUTPUT_PATH}")


if __name__ == "__main__":
    run_etl()
