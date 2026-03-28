"""
DAG: greenhub_gcp_pipeline

Pipeline cloud de ingesta por lotes:
  Task 1  — upload_to_gcs               : sube los Parquet procesados a Cloud Storage
  Task 2a — create_biglake_table        : CREATE OR REPLACE EXTERNAL TABLE samples (Data Lake)
  Task 2b — create_devices_biglake      : CREATE OR REPLACE EXTERNAL TABLE devices (Data Lake)
  Task 3a — load_to_bigquery            : CTAS samples particionada y clustered (Data Warehouse)
  Task 3b — load_devices_to_bigquery    : CTAS devices clustered por brand (Data Warehouse)
  Task 4a — create_view_battery_by_charger   : Vista dashboard Tile 1 (barras apiladas)
  Task 4b — create_view_battery_level_daily  : Vista dashboard Tile 2 (serie temporal)

Arquitectura:
  GCS (Parquet) ──► BigQuery External Table (BigLake) ──► BigQuery Native Table ──► Vistas Dashboard
  [Data Lake]                                             [Data Warehouse]           [Looker Studio]

Dependencias:
  upload_to_gcs >> create_biglake_table    >> load_to_bigquery >> [create_view_charger, create_view_battery_daily]
  upload_to_gcs >> create_devices_biglake >> load_devices_to_bigquery
"""

from __future__ import annotations

import glob
import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# ── Configuración GCP ─────────────────────────────────────────────────────────
GCS_BUCKET     = os.environ.get("GCS_BUCKET",     "greenhub-raw-kestra-2026")
BQ_DATASET     = os.environ.get("BQ_DATASET",     "greenhub")
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "kestra-sandbox-2026")
GCP_CONN_ID    = "google_cloud_default"

PROCESSED_DIR = "/opt/airflow/data/processed"

# ── SQL: tabla externa sobre GCS (Data Lake) ──────────────────────────────────
SQL_CREATE_EXTERNAL = f"""
CREATE OR REPLACE EXTERNAL TABLE `{GCP_PROJECT_ID}.{BQ_DATASET}.samples_external`
OPTIONS (
  format = 'PARQUET',
  uris   = ['gs://{GCS_BUCKET}/samples/*.parquet']
);
"""

# ── SQL: tabla nativa particionada y clustered (Data Warehouse) ───────────────
SQL_CTAS = f"""
CREATE OR REPLACE TABLE `{GCP_PROJECT_ID}.{BQ_DATASET}.samples`
PARTITION BY DATE(timestamp)
CLUSTER BY battery_state, charger
AS
SELECT * FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.samples_external`;
"""

# ── SQL: tabla externa devices sobre GCS (Data Lake) ─────────────────────────
SQL_CREATE_DEVICES_EXTERNAL = f"""
CREATE OR REPLACE EXTERNAL TABLE `{GCP_PROJECT_ID}.{BQ_DATASET}.devices_external`
OPTIONS (
  format = 'PARQUET',
  uris   = ['gs://{GCS_BUCKET}/devices/*.parquet']
);
"""

# ── SQL: tabla nativa devices clustered por brand (Data Warehouse) ────────────
SQL_LOAD_DEVICES = f"""
CREATE OR REPLACE TABLE `{GCP_PROJECT_ID}.{BQ_DATASET}.devices`
CLUSTER BY brand
AS
SELECT * FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.devices_external`;
"""

# ── SQL: vista Dashboard Tile 1 ───────────────────────────────────────────────
# Distribución del estado de batería por cargador agrupada por día.
# Incluye `date` para el filtro de rango de fechas interactivo en Looker Studio.
SQL_VIEW_BATTERY_BY_CHARGER = f"""
CREATE OR REPLACE VIEW `{GCP_PROJECT_ID}.{BQ_DATASET}.v_battery_by_charger_daily`
AS
SELECT
    date,
    charger,
    battery_state,
    COUNT(*)                           AS registros,
    COUNT(DISTINCT device_id)          AS dispositivos_unicos,
    ROUND(AVG(battery_level), 2)       AS battery_level_promedio
FROM
    `{GCP_PROJECT_ID}.{BQ_DATASET}.samples`
WHERE
    charger       IS NOT NULL
    AND battery_state IS NOT NULL
    AND date          IS NOT NULL
GROUP BY
    date,
    charger,
    battery_state;
"""

# ── SQL: vista Dashboard Tile 2 ───────────────────────────────────────────────
# Evolución diaria del nivel de batería promedio por estado de batería.
# La columna `date` es el eje X del gráfico de líneas Y el campo del filtro
# de rango de fechas global del dashboard.
SQL_VIEW_BATTERY_LEVEL_DAILY = f"""
CREATE OR REPLACE VIEW `{GCP_PROJECT_ID}.{BQ_DATASET}.v_battery_level_daily`
AS
SELECT
    date,
    battery_state,
    ROUND(AVG(battery_level), 2)       AS battery_level_promedio,
    ROUND(AVG(up_time) / 3600.0, 1)   AS promedio_horas_encendido,
    COUNT(*)                           AS registros,
    COUNT(DISTINCT device_id)          AS dispositivos_unicos
FROM
    `{GCP_PROJECT_ID}.{BQ_DATASET}.samples`
WHERE
    battery_level IS NOT NULL
    AND battery_state IS NOT NULL
    AND date          IS NOT NULL
GROUP BY
    date,
    battery_state;
"""


# ═══════════════════════════════ TASK FUNCTIONS ═══════════════════════════════

def _upload_to_gcs() -> None:
    """
    Sube los Parquet procesados locales al bucket de Cloud Storage.

    Descubre archivos dinámicamente con glob para manejar los nombres
    generados por Spark (part-00000-{uuid}-c000.snappy.parquet).

    Archivos subidos:
      - devices/*.parquet   (tabla de dimensión completa)
      - samples/*.parquet   (primeros 10 archivos, ~21M filas)
    """
    from google.cloud import storage

    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)

    # TIP DE RENDIMIENTO: antes de subir cada archivo se verifica si ya existe
    # en GCS con blob.exists(). Si el blob está presente, se omite el upload.
    # Esto hace que re-ejecuciones del DAG (por fallo en tasks posteriores,
    # re-triggers manuales, etc.) saltean el upload y terminan en segundos
    # en lugar de los ~2.5 minutos que toma subir todos los archivos desde cero.
    def _upload_if_not_exists(blob, local_path: str, gcs_path: str) -> bool:
        """Sube el archivo solo si no existe en GCS. Retorna True si se subió."""
        if blob.exists():
            print(f"  ⏭ Ya existe, omitiendo: gs://{GCS_BUCKET}/{gcs_path}")
            return False
        blob.upload_from_filename(local_path)
        print(f"  ✓ {local_path} → gs://{GCS_BUCKET}/{gcs_path}")
        return True

    # Devices: tabla de dimensión completa (pocos archivos)
    device_files = sorted(glob.glob(f"{PROCESSED_DIR}/devices/*.parquet"))
    if not device_files:
        raise FileNotFoundError(f"No se encontraron archivos Parquet en {PROCESSED_DIR}/devices/")

    for local_path in device_files:
        gcs_path = f"devices/{os.path.basename(local_path)}"
        _upload_if_not_exists(bucket.blob(gcs_path), local_path, gcs_path)

    # Samples: primeros 10 archivos (~21M filas)
    sample_files = sorted(glob.glob(f"{PROCESSED_DIR}/samples/*.parquet"))[:10]
    if not sample_files:
        raise FileNotFoundError(f"No se encontraron archivos Parquet en {PROCESSED_DIR}/samples/")

    for local_path in sample_files:
        gcs_path = f"samples/{os.path.basename(local_path)}"
        _upload_if_not_exists(bucket.blob(gcs_path), local_path, gcs_path)

    total = len(device_files) + len(sample_files)
    print(f"✓ Upload completo: {total} archivos revisados → gs://{GCS_BUCKET}/")


# ═══════════════════════════════ DAG ══════════════════════════════════════════

default_args = {
    "owner": "airflow",
    "retries": 1,
}

with DAG(
    dag_id="greenhub_gcp_pipeline",
    description="GreenHub Farmer: Parquet procesado → GCS → BigLake → BigQuery",
    schedule_interval="@once",
    start_date=datetime(2026, 3, 28),
    catchup=False,
    default_args=default_args,
    tags=["greenhub", "gcp", "bigquery"],
) as dag:

    # ── Task 1: subir Parquet locales a Cloud Storage ─────────────────────────
    upload_to_gcs = PythonOperator(
        task_id="upload_to_gcs",
        python_callable=_upload_to_gcs,
    )

    # ── Task 2: crear tabla externa BigLake sobre los Parquet en GCS ──────────
    create_biglake_table = BigQueryInsertJobOperator(
        task_id="create_biglake_table",
        configuration={
            "query": {
                "query": SQL_CREATE_EXTERNAL,
                "useLegacySql": False,
            }
        },
        project_id=GCP_PROJECT_ID,
        gcp_conn_id=GCP_CONN_ID,
    )

    # ── Task 3a: CTAS samples particionada y clustered → Data Warehouse ────────
    load_to_bigquery = BigQueryInsertJobOperator(
        task_id="load_to_bigquery",
        configuration={
            "query": {
                "query": SQL_CTAS,
                "useLegacySql": False,
            }
        },
        project_id=GCP_PROJECT_ID,
        gcp_conn_id=GCP_CONN_ID,
    )

    # ── Task 2b: tabla externa BigLake para devices ───────────────────────────
    create_devices_biglake = BigQueryInsertJobOperator(
        task_id="create_devices_biglake",
        configuration={
            "query": {
                "query": SQL_CREATE_DEVICES_EXTERNAL,
                "useLegacySql": False,
            }
        },
        project_id=GCP_PROJECT_ID,
        gcp_conn_id=GCP_CONN_ID,
    )

    # ── Task 3b: tabla nativa devices clustered por brand → Data Warehouse ────
    load_devices_to_bigquery = BigQueryInsertJobOperator(
        task_id="load_devices_to_bigquery",
        configuration={
            "query": {
                "query": SQL_LOAD_DEVICES,
                "useLegacySql": False,
            }
        },
        project_id=GCP_PROJECT_ID,
        gcp_conn_id=GCP_CONN_ID,
    )

    # ── Task 4a: vista Tile 1 — distribución batería por cargador (Dashboard) ─
    create_view_charger = BigQueryInsertJobOperator(
        task_id="create_view_battery_by_charger",
        configuration={
            "query": {
                "query": SQL_VIEW_BATTERY_BY_CHARGER,
                "useLegacySql": False,
            }
        },
        project_id=GCP_PROJECT_ID,
        gcp_conn_id=GCP_CONN_ID,
    )

    # ── Task 4b: vista Tile 2 — evolución nivel de batería diario (Dashboard) ─
    create_view_battery_daily = BigQueryInsertJobOperator(
        task_id="create_view_battery_level_daily",
        configuration={
            "query": {
                "query": SQL_VIEW_BATTERY_LEVEL_DAILY,
                "useLegacySql": False,
            }
        },
        project_id=GCP_PROJECT_ID,
        gcp_conn_id=GCP_CONN_ID,
    )

    upload_to_gcs >> create_biglake_table    >> load_to_bigquery
    upload_to_gcs >> create_devices_biglake >> load_devices_to_bigquery
    # Las vistas dependen de que samples esté cargada (tabla nativa completa)
    load_to_bigquery >> [create_view_charger, create_view_battery_daily]
