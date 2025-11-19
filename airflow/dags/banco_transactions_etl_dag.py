from datetime import datetime
from pathlib import Path
import csv

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


# Ruta base = carpeta de este archivo dentro del contenedor (/opt/airflow/dags)
DAGS_DIR = Path(__file__).resolve().parent
RAW_PATH = DAGS_DIR / "data" / "raw" / "transactions.csv"
OUTPUT_PATH = DAGS_DIR / "data" / "processed" / "sender_debits_summary.csv"


def etl_transactions():
    if not RAW_PATH.exists():
        raise FileNotFoundError(f"No se encontró el CSV en {RAW_PATH}")

    totals: dict[str, dict[str, float | int]] = {}

    with RAW_PATH.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # No filtramos por Transaction Type por ahora
            customer = (row.get("Sender Account ID") or "").strip()
            if not customer:
                continue

            try:
                amount = float(row.get("Transaction Amount") or "0")
            except ValueError:
                continue

            agg = totals.setdefault(customer, {"sum": 0.0, "count": 0})
            agg["sum"] += amount
            agg["count"] += 1

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_PATH.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            ["Sender Account ID", "total_amount", "avg_amount", "transaction_count"]
        )

        for customer, agg in totals.items():
            total = agg["sum"]
            count = agg["count"]
            avg = total / count if count else 0.0
            writer.writerow([customer, f"{total:.2f}", f"{avg:.2f}", count])

    print(f"ETL completado. Archivo generado en: {OUTPUT_PATH}")


with DAG(
    dag_id="banco_transactions_etl",
    description="ETL simple de transacciones bancarias ficticias (tipo banco)",
    start_date=datetime(2025, 1, 1),
    schedule=None,  # lo ejecutás a mano
    catchup=False,
    tags=["banco", "etl"],
):
    etl_task = PythonOperator(
        task_id="run_etl_transactions",
        python_callable=etl_transactions,
    )
