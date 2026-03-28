"""
DAG: greenhub_local_pipeline

Pipeline local de ingesta por lotes:
  Task 1 — kaggle_download  : descarga el dataset de Kaggle (.parquet)
  Task 2 — spark_transform  : PySpark limpia y enriquece los datos con SparkSQL
  Task 3 — postgres_load    : carga el resultado en PostgreSQL (samples + devices)

Dependencias:
  kaggle_download >> spark_transform >> postgres_load
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# ── Rutas dentro del contenedor de Airflow ────────────────────────────────────
RAW_DIR       = "/opt/airflow/data/raw"
PROCESSED_DIR = "/opt/airflow/data/processed"
KAGGLE_DATASET = "hmatalonga/greenhub-farmer"


# ═══════════════════════════════ TASK FUNCTIONS ═══════════════════════════════

def _spark_transform() -> None:
    """
    Lee samples/*.parquet y devices/*.parquet con PySpark (modo local[*]).
    Aplica transformaciones con SparkSQL:
      - Castea timestamp a TIMESTAMP
      - Extrae date, year, month, hour para facilitar consultas del dashboard
      - Filtra filas con timestamp o device_id nulos
    Escribe el resultado como Parquet procesado en /opt/airflow/data/processed/.
    """
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName("GreenHubFarmer-Transform")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    # ── Leer archivos crudos ──────────────────────────────────────────────────
    # El dataset de Kaggle viene como directorios de part-files, e.g.:
    #   data/raw/samples/part-0000.parquet, part-0001.parquet, ...
    #   data/raw/devices/part-0000.parquet, ...
    # PySpark lee todos los part-files de un directorio automáticamente.
    samples_raw = spark.read.parquet(f"{RAW_DIR}/samples/")
    devices_raw = spark.read.parquet(f"{RAW_DIR}/devices/")

    print(f"samples_raw  → {samples_raw.count():,} filas, {len(samples_raw.columns)} columnas")
    print(f"devices_raw  → {devices_raw.count():,} filas, {len(devices_raw.columns)} columnas")

    # ── Registrar vistas temporales para SparkSQL ─────────────────────────────
    samples_raw.createOrReplaceTempView("samples_raw")

    # ── Transformación con SparkSQL ───────────────────────────────────────────
    # Esquema real verificado con inspect_schema.py:
    #   - `usage`        → renombrado a cpu_usage
    #   - `memory_user`  → renombrado a memory_used
    #   - `free`/`total` → renombrados a storage_free/storage_total
    #   - timestamp ya es TIMESTAMP (no se necesita CAST)
    samples_clean = spark.sql("""
        SELECT
            id,
            device_id,
            timestamp,
            TO_DATE(timestamp)   AS date,
            YEAR(timestamp)      AS year,
            MONTH(timestamp)     AS month,
            HOUR(timestamp)      AS hour,
            battery_state,
            charger,
            battery_level,
            health,
            voltage,
            temperature,
            usage                AS cpu_usage,
            memory_active,
            memory_inactive,
            memory_free,
            memory_user          AS memory_used,
            free                 AS storage_free,
            total                AS storage_total,
            free_system,
            total_system,
            up_time,
            sleep_time,
            network_status,
            network_type,
            mobile_network_type,
            mobile_data_status,
            mobile_data_activity,
            wifi_status,
            wifi_signal_strength,
            wifi_link_speed,
            screen_on,
            screen_brightness,
            roaming_enabled,
            bluetooth_enabled,
            location_enabled,
            power_saver_enabled,
            nfc_enabled,
            developer_mode,
            timezone,
            country_code
        FROM samples_raw
        WHERE timestamp IS NOT NULL
          AND device_id  IS NOT NULL
    """)

    # ── Guardar resultado como Parquet ────────────────────────────────────────
    # Spark escribe múltiples part-files (uno por partición RDD)
    samples_clean.write.mode("overwrite").parquet(f"{PROCESSED_DIR}/samples/")
    devices_raw.write.mode("overwrite").parquet(f"{PROCESSED_DIR}/devices/")

    total = samples_clean.count()
    print(f"✓ samples_clean escritos: {total:,} filas")
    spark.stop()


def _postgres_load() -> None:
    """
    Carga los Parquet procesados en PostgreSQL:
      - devices  → tabla de dimensión (replace completo en cada ejecución)
      - samples  → tabla de hechos (replace completo, cargada en micro-batches)
    Usa PyArrow iter_batches() para evitar OOM al cargar 49M filas.
    Crea un índice compuesto (battery_state, timestamp) para acelerar el dashboard.
    """
    import gc
    import pandas as pd
    import pyarrow.parquet as pq
    from sqlalchemy import create_engine, text

    pg_user = os.environ.get("POSTGRES_USER", "postgres")
    pg_pass = os.environ.get("POSTGRES_PASSWORD", "postgres")
    pg_db   = os.environ.get("POSTGRES_DB", "greenhub")

    engine = create_engine(
        f"postgresql+psycopg2://{pg_user}:{pg_pass}@postgres:5432/{pg_db}",
        pool_pre_ping=True,
    )

    # ── devices (dimensión — tabla pequeña, replace completo) ─────────────────
    devices_df = pd.read_parquet(f"{PROCESSED_DIR}/devices/")
    devices_df.to_sql("devices", engine, if_exists="replace", index=False, chunksize=1000)
    print(f"✓ devices cargados: {len(devices_df):,} filas")
    del devices_df
    gc.collect()

    # ── samples (hecho — tabla grande, micro-batches de 50K filas) ────────────
    # iter_batches() de PyArrow nunca carga el archivo completo en RAM:
    # lee el parquet en row-groups y convierte a pandas de a 50K filas.
    samples_dir = f"{PROCESSED_DIR}/samples/"
    part_files = sorted([
        os.path.join(samples_dir, f)
        for f in os.listdir(samples_dir)
        if f.endswith(".parquet")
    ])

    if not part_files:
        raise FileNotFoundError(f"No se encontraron archivos Parquet en {samples_dir}")

    # Crear tabla vacía con el schema correcto antes del primer append
    first_batch = next(pq.ParquetFile(part_files[0]).iter_batches(batch_size=1))
    schema_df = first_batch.to_pandas().head(0)
    schema_df.to_sql("samples", engine, if_exists="replace", index=False)
    del schema_df, first_batch
    gc.collect()

    total = 0
    BATCH_SIZE = 50_000   # filas por batch — ajusta si aún hay OOM
    for i, part in enumerate(part_files):
        pf = pq.ParquetFile(part)
        for batch in pf.iter_batches(batch_size=BATCH_SIZE):
            batch_df = batch.to_pandas()
            batch_df.to_sql("samples", engine, if_exists="append", index=False, chunksize=5000)
            total += len(batch_df)
            del batch_df
            gc.collect()
        print(f"  [{i+1}/{len(part_files)}] → {total:,} filas cargadas...")

    # ── Índice para acelerar consultas del dashboard ───────────────────────────
    with engine.connect() as conn:
        conn.execute(text(
            "CREATE INDEX IF NOT EXISTS idx_samples_battery_ts "
            "ON samples (battery_state, timestamp)"
        ))

    print(f"✓ samples cargados: {total:,} filas")
    print("✓ Índice creado: (battery_state, timestamp)")


# ═══════════════════════════════ DEFINICIÓN DEL DAG ══════════════════════════

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="greenhub_local_pipeline",
    default_args=default_args,
    description="GreenHub Farmer: Kaggle → PySpark (SparkSQL) → PostgreSQL",
    schedule_interval="@once",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["greenhub", "local", "pyspark", "postgres"],
) as dag:

    # ── Task 1: descarga el dataset de Kaggle ─────────────────────────────────
    t1_download = BashOperator(
        task_id="kaggle_download",
        bash_command=f"""
            set -e
            mkdir -p {RAW_DIR}

            # Workaround bug kaggle>=1.7: makedirs falla si el dir ya existe.
            # Limpiamos y recreamos para que el import lo encuentre vacío.
            rm -rf ~/.config/kaggle
            mkdir -p ~/.config/kaggle

            echo "Descargando dataset {KAGGLE_DATASET} de Kaggle..."
            kaggle datasets download \
                -d {KAGGLE_DATASET} \
                --path {RAW_DIR} \
                --unzip \
                --force
            echo "Archivos en {RAW_DIR}:"
            ls -lh {RAW_DIR}/
        """,
    )

    # ── Task 2: transformación con PySpark + SparkSQL ─────────────────────────
    t2_transform = PythonOperator(
        task_id="spark_transform",
        python_callable=_spark_transform,
    )

    # ── Task 3: carga en PostgreSQL ───────────────────────────────────────────
    t3_load = PythonOperator(
        task_id="postgres_load",
        python_callable=_postgres_load,
    )

    # ── Flujo de dependencias ─────────────────────────────────────────────────
    t1_download >> t2_transform >> t3_load
