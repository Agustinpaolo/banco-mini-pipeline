# Banco Mini Pipeline

Proyecto personal de aprendizaje para practicar conceptos básicos de ingeniería de datos:

- Orquestación de un pipeline ETL con **Apache Airflow**.
- Transformación de datos en **Python “clásico”** (módulo ETL simple).
- Transformación equivalente usando **PySpark** como motor distribuido.
- Uso de un dataset de transacciones bancarias ficticias en formato CSV.

El objetivo es tener un proyecto pequeño pero completo, que pueda mostrarse en un portfolio como ejemplo de pipeline de datos de punta a punta.

---

## Puntos clave del proyecto

- 🧠 **Separación de responsabilidades**  
  El DAG de Airflow solo orquesta; la lógica de negocio vive en módulos Python externos.

- 🐍 **ETL simple en Python**  
  Lectura de CSV, agregación por cliente (`Sender Account ID`) y escritura de un resumen procesado.

- ⚡ **Versión equivalente en PySpark**  
  Se replica la lógica del ETL simple usando Spark, con esquema inferido y funciones de agregación distribuidas.

- 📂 **Estructura clara de datos**  
  Datos crudos en `data/raw/` y resultados en `data/processed/`.

- 🧪 **Dataset pequeño incluido en el repo**  
  El CSV pesa ~130 KB, ideal para pruebas rápidas sin depender de fuentes externas.

---

## Arquitectura general

### 1. Airflow + ETL en Python

- El DAG principal se define en `dags/banco_etl_dag.py` y crea un DAG llamado `banco_transactions_etl`.  
- Este DAG llama a la función `etl_transactions` definida en `dags/etl_transactions.py`.  
- La función:
  - Lee el archivo CSV de transacciones crudo.
  - Agrega montos y cantidades por `Sender Account ID`.
  - Genera un CSV procesado con métricas por cliente.

Dentro del contenedor de Airflow, el código espera encontrar:

- Entrada: `/opt/airflow/data/raw/transactions.csv`  
- Salida: `/opt/airflow/data/processed/sender_debits_summary.csv`

El mapeo de volúmenes (host → contenedor) se configura en `docker-compose.yaml`.

### 2. ETL con PySpark

- El script de Spark está en `spark/spark_transform.py`.
- Se ejecuta de forma independiente (no desde Airflow).
- Hace, a grandes rasgos, lo mismo que el ETL simple:
  - Lee `data/raw/transactions.csv` desde el sistema de archivos local.
  - Normaliza `Transaction Type` (minúsculas, sin espacios).
  - Filtra solo transacciones de débito (`withdrawal`, `transfer`).
  - Convierte `Transaction Amount` a numérico y descarta montos inválidos.
  - Calcula:
    - `total_debit`
    - `avg_debit`
    - `debit_count`
  - Escribe el resultado en `data/processed/sender_debits_summary_spark.csv`.

---

## Dataset de transacciones

El proyecto utiliza un archivo CSV con transacciones bancarias ficticias:

- Ruta (host): `data/raw/transactions.csv`
- Tamaño aproximado: ~130 KB
- Columnas:

```text
Transaction ID,
Sender Account ID,
Receiver Account ID,
Transaction Amount,
Transaction Type,
Timestamp,
Transaction Status,
Fraud Flag,
Geolocation (Latitude/Longitude),
Device Used,
Network Slice ID,
Latency (ms),
Slice Bandwidth (Mbps),
PIN Code
```

Las transformaciones se centran principalmente en:

- `Sender Account ID`
- `Transaction Amount`
- `Transaction Type`

---

## Estructura del repositorio

Estructura aproximada:

```text
banco-mini-pipeline/
├── dags/
│   ├── banco_etl_dag.py          # DAG de Airflow
│   └── etl_transactions.py       # ETL simple en Python
├── spark/
│   └── spark_transform.py        # ETL equivalente en PySpark
├── data/
│   ├── raw/
│   │   └── transactions.csv      # Dataset de entrada (incluido en el repo)
│   └── processed/
│       ├── sender_debits_summary.csv
│       └── sender_debits_summary_spark.csv
├── docker-compose.yaml           # Orquestación de Airflow con Docker
├── requirements.txt              # Dependencias para el script de Spark (pyspark)
└── README.md
```

> Nota: la carpeta `data/processed/` se genera al ejecutar los ETL (Python y Spark).

---

## Requisitos previos

- **Docker Desktop** (o Docker Engine) instalado y funcionando.
- **Docker Compose** (en versiones nuevas ya viene como `docker compose`).
- **Python 3.10+** (para ejecutar el ETL de Spark desde el host).
- **Java** (JDK o JRE) instalado y accesible si PySpark lo requiere en tu entorno local.

---

## Puesta en marcha rápida

### 1. Clonar el repositorio

```bash
git clone https://github.com/<tu-usuario>/banco-mini-pipeline.git
cd banco-mini-pipeline
```

(Ajustá la URL según tu GitHub real.)

---

## Ejecutar el ETL con Airflow (Docker Compose)

1. Asegurate de tener **Docker Desktop** levantado.
2. Desde la raíz del proyecto, levantá Airflow con Docker Compose:

```bash
docker compose up -d
```

> También podés hacerlo desde la GUI de Docker Desktop si preferís, pero el README documenta la variante por línea de comando.

3. Verificá que los contenedores se estén ejecutando:

```bash
docker compose ps
```

4. Asegurate de que el archivo `data/raw/transactions.csv` exista en el host.  
   El `docker-compose.yaml` debe montar la carpeta local `./data` dentro del contenedor de Airflow (por ejemplo en `/opt/airflow/data/`).

5. Entrá a la interfaz web de Airflow (puerto definido en `docker-compose.yaml`, típicamente 8080 si usaste la plantilla oficial).

6. En la UI de Airflow:

   - Localizá el DAG `banco_transactions_etl`.
   - Activá el DAG si está pausado.
   - Lanzá una ejecución manual (“Trigger DAG”).

7. Una vez terminada la ejecución, deberías ver un archivo similar a:

```text
data/processed/sender_debits_summary.csv
```

con columnas como:

```text
Sender Account ID,total_amount,avg_amount,transaction_count
```

---

## Ejecutar el ETL con PySpark

El ETL de Spark se ejecuta directamente desde el host, fuera de Airflow.

### 1. Crear y activar un entorno virtual (opcional pero recomendado)

```bash
python -m venv .venv
source .venv/bin/activate    # En Windows: .venv\Scriptsactivate
```

### 2. Instalar dependencias

```bash
pip install -r requirements.txt
```

> Actualmente `requirements.txt` contiene `pyspark`, suficiente para este script.

### 3. Ejecutar el script de Spark

Desde la raíz del proyecto:

```bash
python spark/spark_transform.py
```

Esto:

- Lee `data/raw/transactions.csv`.
- Ejecuta la transformación con PySpark.
- Genera:

```text
data/processed/sender_debits_summary_spark.csv
```

---

## Comparación entre ETL simple y ETL en Spark

Ambos pipelines producen un resumen por `Sender Account ID` con métricas muy similares, pero:

- El ETL simple en Python:
  - Es más directo y fácil de leer.
  - Ideal para datasets pequeños y para explicar la lógica paso a paso.

- El ETL en PySpark:
  - Escala mejor a volúmenes grandes de datos.
  - Conocimiento básico de Spark:
    - lectura de CSV con esquema inferido,
    - uso de `withColumn`, `filter`, `groupBy` y funciones de agregación,
    - conversión del resultado a pandas solo al final (por simplicidad en este proyecto).

---

## Ideas de mejora (trabajo futuro)

Algunas extensiones posibles para seguir aprendiendo:

- Añadir tests unitarios para la lógica de agregación.
- Parametrizar rutas de entrada/salida vía variables de entorno o `airflow.Variable`.

---

## Estado del proyecto

Proyecto en desarrollo como parte de mi camino de aprendizaje hacia roles de **Data Engineer Jr.** y proyectos de datos más complejos.  
El objetivo principal es demostrar comprensión de:

- Orquestación de ETLs con Airflow.
- Transformaciones básicas con Python.
- Uso inicial de PySpark en un flujo reproducible.
