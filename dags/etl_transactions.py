"""
Procesa transacciones bancarias mediante un ETL sencillo.

Define la función `etl_transactions`, que lee un CSV crudo, agrega montos y
cantidades por 'Sender Account ID' y genera un archivo CSV procesado con
métricas agregadas. El objetivo es mostrar un ETL simple y desacoplado del
DAG de Airflow.
"""

from pathlib import Path
import csv
import logging

# Ruta base dentro del contenedor Airflow
BASE_DIR = Path("/opt/airflow")

RAW_PATH = BASE_DIR / "data" / "raw" / "transactions.csv"
OUTPUT_PATH = BASE_DIR / "data" / "processed" / "sender_debits_summary.csv"

logging.basicConfig(level=logging.INFO)


def etl_transactions() -> None:
    """
    Ejecuta un ETL simple sobre transacciones bancarias.

    Pasos:
    1. Lee el archivo CSV crudo desde RAW_PATH.
    2. Agrupa por 'Sender Account ID'.
    3. Calcula total, promedio y cantidad de transacciones por cliente.
    4. Escribe un CSV procesado con las métricas agregadas en OUTPUT_PATH.

    La función no retorna valores; produce un archivo de salida.
    Lanza FileNotFoundError si el archivo de entrada no existe.
    """

    logging.info(f"Inicio del ETL. Leyendo archivo: {RAW_PATH}")

    if not RAW_PATH.exists():
        raise FileNotFoundError(f"Archivo de entrada no encontrado: {RAW_PATH}")

    totals: dict[str, dict[str, float | int]] = {}

    with RAW_PATH.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            customer = (row.get("Sender Account ID") or "").strip()
            if not customer:
                continue  # dato inválido

            # Intentamos parsear el monto
            try:
                amount = float(row.get("Transaction Amount") or "0")
            except ValueError:
                logging.warning(f"Monto inválido encontrado: {row.get('Transaction Amount')}")
                continue

            agg = totals.setdefault(customer, {"sum": 0.0, "count": 0})
            agg["sum"] += amount
            agg["count"] += 1

    # Creamos carpeta si no existe
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

    logging.info(f"ETL completado con éxito. Archivo generado en: {OUTPUT_PATH}")
