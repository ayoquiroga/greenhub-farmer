# GreenHub Farmer — Proyecto de Ingeniería de Datos

Pipeline de datos de extremo a extremo construido como proyecto final del curso **Data Engineering Zoomcamp 2026** de DataTalks.Club.

---

## Descripción del Problema

El dataset [GreenHub Farmer](https://www.kaggle.com/datasets/hmatalonga/greenhub-farmer/data) contiene mediciones de sensores de smartphones recolectadas en granjas. Incluye datos de batería, CPU, memoria RAM, red y GPS de múltiples dispositivos a lo largo del tiempo.

**Objetivo:** construir un pipeline que procese este dataset (~3 GB) y lo exponga en un Dashboard interactivo con 2 visualizaciones, permitiendo consultas eficientes sobre millones de registros.

### Proveniencia del Dataset

> **Sources**
> The GreenHub Farmer dataset was established through the GreenHub initiative, a collaborative research effort involving several universities in Portugal and Brazil to study mobile energy consumption. The data is gathered via continuous crowdsourcing using an open-source mobile application called **BatteryHub**, which tracks system event broadcasts—such as battery state changes—to capture snapshots of a device's current state.
>
> **Collection Methodology**
> This collection methodology is designed to be anonymous, ensuring that no personal information, such as phone numbers or locations, is recorded. By leveraging institutional media outlets to attract users, the initiative successfully compiled a heterogeneous repository.
>
> **Citation**
> *GreenHub Farmer: Real-World Data for Android Energy Mining*

---

**Modelo de datos (estrella):**
```
devices (dimensión)
    └── device_id ──► samples (hecho)
                          ├── timestamp       → PARTITION BY
                          ├── battery_state   → CLUSTER BY / INDEX
                          ├── charger
                          ├── battery_level
                          ├── cpu_usage
                          └── memory_*
```

---

## Arquitectura del Pipeline

```
Dataset Kaggle (.parquet ~3GB)
        │
        ▼
┌─────────────────────────────────────────────────────────────┐
│                    OPCIÓN A — LOCAL                         │
│                                                             │
│  Airflow DAG  ──►  Spark SQL  ──►  PostgreSQL (Docker)      │
│  (orquesta)       (transforma)     (particionado/indexado)  │
│                                           │                 │
│                                           ▼                 │
│                                    Looker Studio            │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│                    OPCIÓN B — CLOUD (GCP)                   │
│                                                             │
│  Local Parquet ──► GCS (BigLake) ──► BigQuery (CTAS)        │
│  (upload local)    (data lake)       (data warehouse)       │
│                    tabla externa     PARTITION + CLUSTER     │
│                                           │                 │
│                                           ▼                 │
│                                    Looker Studio            │
└─────────────────────────────────────────────────────────────┘
        ▲
        │
   Terraform (IaC) provisiona toda la infraestructura GCP
```

---

## Tecnologías Utilizadas

| Componente | Tecnología |
|---|---|
| Lenguaje | Python 3.12 |
| Gestión de entorno | `uv` |
| Infraestructura como Código | Terraform |
| Contenedores | Docker + Docker Compose |
| Orquestación | Apache Airflow 2.9 |
| Procesamiento local | Apache Spark (SparkSQL) |
| Base de datos local | PostgreSQL 16 |
| UI de base de datos | pgAdmin 4 |
| Exploración de datos | Jupyter Notebooks + DuckDB |
| Data Lake (cloud) | Google Cloud Storage (BigLake) |
| Data Warehouse (cloud) | BigQuery |
| Procesamiento cloud | BigQuery CTAS (reemplaza Dataflow — ver decisión técnica) |
| Dashboard | Looker Studio (+ Streamlit como bonus) |
| Control de versiones | Git + GitHub |

---

## Estructura del Proyecto

```
GreenHubFarmer/
│
├── terraform/                  # IaC — infraestructura GCP
│   ├── main.tf
│   ├── variables.tf
│   └── outputs.tf
│
├── docker/                     # Dockerfiles de contenedores custom
│   └── airflow/
│       └── Dockerfile          # Airflow + PySpark + Kaggle CLI
│
├── docker-compose.yml          # Orquesta los 4 contenedores
│
├── dags/                       # DAGs de Airflow
│   ├── local_pipeline_dag.py   # Opción A: Kaggle → Spark → PostgreSQL
│   └── gcp_pipeline_dag.py     # Opción B: Local → GCS (BigLake) → BigQuery (CTAS)
│
├── scripts/                    # Scripts Python auxiliares
│
├── notebooks/                  # Exploración y desarrollo
│   └── 01_explore_dataset.ipynb
│
├── sql/                        # DDL para PostgreSQL y BigQuery
│
├── data/
│   ├── raw/                    # Datos descargados (ignorado en git)
│   └── processed/              # Datos transformados (ignorado en git)
│
├── .env                        # Variables de entorno (ignorado en git)
├── .gitignore
├── .python-version             # Python 3.12
├── pyproject.toml              # Dependencias del proyecto (uv)
└── README.md
```

---

## Prerrequisitos

- Docker Desktop (activo)
- Python 3.12+ con `uv` instalado
- Terraform
- Cuenta de Kaggle con API key configurada en `~/.kaggle/kaggle.json`
- Cuenta de GCP con cuenta de servicio (archivo `.json` **no incluido en git**)

---

## Paso 1 — Configuración del Entorno Local

Se configuró el entorno de trabajo con Python 3.12 y el gestor de paquetes `uv`.

### Archivos creados

- `.python-version` → fija la versión Python 3.12 para el proyecto
- `pyproject.toml` → declara todas las dependencias del proyecto
- `.gitignore` → protege credenciales, datos y estado de Terraform

### Dependencias del proyecto (`pyproject.toml`)

```toml
[dependencies]
pandas = ">=3.0.0"
pyarrow = ">=23.0.0"
duckdb = ">=1.2.0"
psycopg2-binary = ">=2.9.11"
sqlalchemy = ">=2.0.46"
google-cloud-storage = ">=2.19.0"
google-cloud-bigquery = ">=3.29.0"
kaggle = ">=1.7.4"
python-dotenv = ">=1.0.0"

[dev-dependencies]
jupyter = ">=1.1.1"
ipykernel = ">=6.29.0"
```

### Comandos ejecutados

```bash
uv venv              # crea el entorno virtual en .venv/
uv sync --dev        # instala todas las dependencias (incluyendo dev)
```

**Decisión técnica:** se usa `uv` por su velocidad de resolución de dependencias y compatibilidad con `pyproject.toml` estándar. El entorno solo se usa para notebooks y scripts auxiliares — Airflow y Spark corren en Docker.

---

## Paso 2 — Sincronización con GitHub

Se inicializó el repositorio Git y se subió a GitHub.

### Comandos ejecutados

```bash
git init
git add .
git commit -m "chore: initial project structure with uv environment"
git remote add origin https://github.com/ayoquiroga/greenhub-farmer.git
git branch -M master
git pull origin master --allow-unrelated-histories  # GitHub crea README por defecto
git push -u origin master
```

### Problema resuelto: conflicto de `.gitignore`

GitHub genera automáticamente un `.gitignore` de Python genérico al inicializar el repositorio. Al intentar hacer `git push`, hubo un conflicto con el `.gitignore` personalizado del proyecto. Se resolvió sobreescribiendo el archivo local con la versión del proyecto y haciendo un nuevo commit.

### Reglas del `.gitignore`

```gitignore
# Entorno virtual
.venv/

# Datos (demasiado grandes para git)
data/raw/
data/processed/
*.parquet
*.csv

# Credenciales (NUNCA en git)
*.json               # clave de cuenta de GCP
!pyproject.toml      # excepción: pyproject.toml sí se incluye

# Terraform
*.tfstate
*.tfstate.backup
terraform.tfvars
```

---

## Paso 3 — Exploración del Dataset

**Notebook:** [notebooks/01_explore_dataset.ipynb](notebooks/01_explore_dataset.ipynb)

### Proceso de exploración

El notebook está organizado en 7 secciones:

| Sección | Descripción |
|---|---|
| 0 | Verificación de credenciales de Kaggle (`~/.kaggle/kaggle.json`) |
| 1 | Descarga del dataset con la API de Kaggle |
| 2 | Listado de archivos Parquet con pandas |
| 3 | `DESCRIBE` y `COUNT(*)` con DuckDB (sin cargar todo en RAM) |
| 4 | Análisis de nulos sobre muestra de 50.000 filas |
| 5 | Identificación de columnas para PARTITION y CLUSTER |
| 6 | Conversión a CSV mediante `COPY` de DuckDB |
| 7 | Conclusiones del esquema de datos |

### Estructura del Dataset (descubierta)

| Archivo | Rol | Descripción |
|---|---|---|
| `samples.parquet` | **Tabla de hechos** | Mediciones de sensores por dispositivo y timestamp |
| `devices.parquet` | **Tabla de dimensiones** | Información estática de cada dispositivo |

> **Nota:** el dataset de Kaggle viene ya en formato Parquet (no CSV como se asumió inicialmente). El notebook fue adaptado al descubrirlo.

### Hallazgos clave

| Decisión | Columna | Justificación |
|---|---|---|
| `PARTITION BY` | `timestamp` | Serie temporal — reduce el escaneo por rango de fechas |
| `CLUSTER BY` / `INDEX` | `battery_state` | 4 valores únicos: Charging/Discharging/Not charging/Full |
| Clave de unión | `device_id` | Relaciona `samples` → `devices` |

### Columnas de `samples` para el Dashboard

```
timestamp        → datetime (PARTITION BY)
battery_state    → string   (CLUSTER BY / INDEX)
charger          → string   (3 valores: unplugged, ac, usb)
battery_level    → float    (0-100%)
cpu_usage        → float    (% de uso del procesador)
memory_free      → float    (MB libres)
memory_used      → float    (MB en uso)
network_type     → string   (WiFi, Mobile, None)
```

### Problemas resueltos

- `read_parquet()` de DuckDB no soporta `SAMPLE_SIZE` (ese parámetro solo existe en `read_csv_auto`). Se eliminó el parámetro y se usó `LIMIT` en su lugar.

---

## Paso 4 — Infraestructura Docker

### Archivos creados

| Archivo | Función |
|---|---|
| `docker-compose.yml` | Orquesta los 5 servicios |
| `docker/airflow/Dockerfile` | Imagen custom de Airflow con Java + PySpark |
| `docker/airflow/requirements.txt` | Dependencias Python adicionales del contenedor |
| `.env.example` | Plantilla de variables de entorno (en git) |
| `.env` | Variables de entorno reales (en `.gitignore`) |

### Arquitectura de contenedores

```
docker-compose.yml
│
├── postgres (postgres:16)              puerto 5432
│   ├── tablas: samples, devices
│   └── healthcheck: pg_isready
│
├── pgadmin (dpage/pgadmin4)            puerto 8085
│   └── depends_on: postgres (healthy)
│
├── airflow-metadata (postgres:16)      puerto interno
│   ├── base de datos interna de Airflow
│   └── healthcheck: pg_isready
│
├── airflow-init (custom)               corre una sola vez
│   ├── airflow db init
│   ├── airflow users create
│   └── depende de: airflow-metadata (healthy)
│
└── airflow (custom Dockerfile)         puerto 8080
    ├── FROM apache/airflow:2.9.0
    ├── Java 17 (requerido por PySpark)
    ├── RUN pip install pyspark kaggle pyarrow psycopg2-binary
    ├── volumenes: dags/, data/, scripts/, logs/
    └── depends_on: airflow-metadata (healthy) + airflow-init (completed)
```

### Decisiones técnicas

**¿Por qué 2 PostgreSQL?** Airflow requiere su propia base de datos para almacenar el estado de DAGs, ejecuciones y conexiones. Es completamente independiente de la base de datos del proyecto.

**¿Por qué Spark en `local[*]` dentro de Airflow?** Para el entorno local no se necesita un clúster Spark separado. El modo `local[*]` usa todos los núcleos de la máquina y simplifica la arquitectura eliminando un contenedor extra.

**¿Por qué `airflow-init` en un contenedor separado?** Separar la inicialización del servidor permite que `airflow-init` corra solo una vez (`restart: on-failure`) mientras el contenedor `airflow` reinicia si falla. Es el patrón recomendado por la documentación oficial.

**`uv` vs `pip` en Docker:** `uv` se usa **solo en el entorno local** (notebooks y scripts). Dentro de los contenedores Docker, se usa `pip` directamente porque las imágenes oficiales de Airflow ya están optimizadas con pip.

### Configuración del `.env`

Antes de levantar los contenedores, completar el `.env`:

```bash
# 1. Generar una Fernet key para Airflow
uv run python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# 2. Copiar el valor al .env en AIRFLOW__CORE__FERNET_KEY

# 3. Completar las credenciales de Kaggle (ver ~/.kaggle/kaggle.json)
#    KAGGLE_USERNAME y KAGGLE_KEY
```

### Comandos para levantar los contenedores

```bash
# Crear la carpeta de logs que monta Airflow
New-Item -ItemType Directory -Force -Path logs    # PowerShell
# mkdir -p logs                                   # bash/Linux/Mac

# Construir la imagen custom y levantar todos los servicios
docker compose up -d
```

### Verificación

| Servicio | URL | Credenciales |
|---|---|---|
| Airflow | http://localhost:8080 | airflow / airflow |
| pgAdmin | http://localhost:8085 | admin@admin.com / root |
| PostgreSQL | localhost:5432 | postgres / postgres (DB: greenhub) |

**Conexión de pgAdmin a PostgreSQL:**
- Host: `postgres` (nombre del servicio en Docker Compose)
- Port: `5432`
- Database: `greenhub`
- Username / Password: los definidos en `.env`

✅ Validado: acceso a Airflow web, pgAdmin web y conexión a PostgreSQL confirmados.

---

## Paso 5 — DAG de Airflow: Pipeline Local

### Archivo creado

**`dags/local_pipeline_dag.py`** — montado automáticamente en el contenedor de Airflow vía volumen Docker; Airflow lo detecta sin necesitar reiniciar.

### Flujo del DAG (`greenhub_local_pipeline`)

```
kaggle_download  ──►  spark_transform  ──►  postgres_load
  BashOperator        PythonOperator        PythonOperator
```

| Task | Operador | Descripción |
|---|---|---|
| `kaggle_download` | `BashOperator` | `kaggle datasets download -d hmatalonga/greenhub-farmer --unzip --force` |
| `spark_transform` | `PythonOperator` | PySpark en modo `local[*]` + **SparkSQL** para limpiar y enriquecer |
| `postgres_load` | `PythonOperator` | Pandas carga los Parquet procesados en PostgreSQL, crea índice |

### Transformaciones SparkSQL (Task 2)

```sql
SELECT
    CAST(timestamp AS TIMESTAMP)            AS timestamp,
    TO_DATE(CAST(timestamp AS TIMESTAMP))   AS date,
    YEAR(CAST(timestamp AS TIMESTAMP))      AS year,
    MONTH(CAST(timestamp AS TIMESTAMP))     AS month,
    HOUR(CAST(timestamp AS TIMESTAMP))      AS hour,
    device_id, battery_state, charger,
    battery_level, cpu_usage,
    memory_free, memory_used, network_type
FROM samples_raw
WHERE timestamp IS NOT NULL
  AND device_id  IS NOT NULL
```

Columnas añadidas: `date`, `year`, `month`, `hour` (claves para el dashboard).

### Carga en PostgreSQL (Task 3)

- **`devices`** → replace completo en cada ejecución (tabla pequeña)
- **`samples`** → replace + append por archivo Parquet (tabla grande, lee un `part-file` a la vez para no agotar RAM)
- **Índice creado:** `CREATE INDEX IF NOT EXISTS idx_samples_battery_ts ON samples (battery_state, timestamp)`

### Actualización de `docker-compose.yml`

Se agregaron las variables de PostgreSQL al bloque `x-airflow-common` para que el DAG pueda conectarse a la base de datos del proyecto:

```yaml
POSTGRES_USER: ${POSTGRES_USER:-postgres}
POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:-postgres}
POSTGRES_DB: ${POSTGRES_DB:-greenhub}
```

### Cómo disparar el DAG

```bash
# Reiniciar los contenedores para que tomen las nuevas env vars
docker compose down && docker compose up -d

# Esperar ~1 minuto y activar el DAG en la UI de Airflow
# → http://localhost:8080  → DAG: greenhub_local_pipeline → Toggle ON → Trigger DAG ▶
```

### Evidencia de ejecución exitosa

**Vista Grid del DAG** — las 3 tareas en verde para las 2 ejecuciones completadas (duración máxima: 6h 20min, promedio: 6h 17min):

![Airflow DAG Grid - greenhub_local_pipeline](docs/images/airflow_dag_grid.png)

**Lista de Jobs** — todos los `LocalTaskJob` con estado `success`; última carga finalizada el `2026-03-28 00:51:14 UTC` (~49M filas cargadas en PostgreSQL):

![Airflow List Job](docs/images/airflow_job_list.png)

**Log en tiempo real de `postgres_load`** — seguimiento del progreso archivo por archivo vía `tail -f`:

```
$ docker exec greenhub-airflow bash -c \
  "tail -f '/opt/airflow/logs/dag_id=greenhub_local_pipeline/run_id=manual__2026-03-27T18:31:05.779561+00:00/task_id=postgres_load/attempt=1.log'" 2>&1

[2026-03-27T19:04:28.021+0000] INFO -   [1/22] →  2,112,699 filas cargadas...
[2026-03-27T19:13:27.900+0000] INFO -   [2/22] →  4,225,398 filas cargadas...
[2026-03-27T19:21:56.475+0000] INFO -   [3/22] →  6,338,097 filas cargadas...
[2026-03-27T19:31:48.456+0000] INFO -   [4/22] →  8,450,796 filas cargadas...
[2026-03-27T19:41:02.744+0000] INFO -   [5/22] → 10,563,495 filas cargadas...
[2026-03-27T19:50:06.620+0000] INFO -   [6/22] → 12,676,194 filas cargadas...
[2026-03-27T19:58:42.604+0000] INFO -   [7/22] → 14,788,893 filas cargadas...
[2026-03-27T20:07:16.239+0000] INFO -   [8/22] → 16,901,592 filas cargadas...
[2026-03-27T20:15:34.308+0000] INFO -   [9/22] → 19,014,291 filas cargadas...
[2026-03-27T20:45:53.522+0000] INFO -  [12/22] → 25,352,388 filas cargadas...
[2026-03-27T20:55:11.713+0000] INFO -  [13/22] → 27,465,087 filas cargadas...
[2026-03-27T21:06:57.477+0000] INFO -  [14/22] → 29,577,786 filas cargadas...
[2026-03-27T21:16:35.970+0000] INFO -  [15/22] → 31,690,485 filas cargadas...
[2026-03-27T21:26:12.160+0000] INFO -  [16/22] → 33,803,189 filas cargadas...
[2026-03-27T21:40:38.300+0000] INFO -  [17/22] → 36,620,121 filas cargadas...
[2026-03-27T21:50:57.973+0000] INFO -  [18/22] → 39,437,053 filas cargadas...
[2026-03-27T22:01:14.485+0000] INFO -  [19/22] → 42,253,985 filas cargadas...
[2026-03-28T00:45:27.512+0000] INFO - ✓ Índice creado: (battery_state, timestamp)
[2026-03-28T00:45:27.516+0000] INFO - Done. Returned value was: None
[2026-03-28T00:45:27.541+0000] INFO - Marking task as SUCCESS. dag_id=greenhub_local_pipeline, task_id=postgres_load, start_date=20260327T185643, end_date=20260328T004527
[2026-03-28T00:45:27.717+0000] INFO - Task exited with return code 0
```

---

## Paso 6 — DAG de Airflow: Pipeline Cloud (GCP)

**Archivo:** `dags/gcp_pipeline_dag.py`

### Decisión técnica: BigLake + BigQuery CTAS en lugar de Dataflow

Dataflow (Apache Beam) no estaba habilitado en el proyecto GCP y su uso tendría costos de procesamiento elevados. Como los datos ya están en formato Parquet (que BigQuery lee nativamente), se reemplaza por la siguiente arquitectura de 3 pasos:

| Paso | Operador Airflow | Descripción |
|---|---|---|
| 1. Upload | `LocalFilesystemToGCSOperator` | Sube 11 archivos Parquet locales a GCS (1 devices + 10 samples) |
| 2. BigLake | `BigQueryCreateExternalTableOperator` | Crea tabla externa sobre GCS → **Data Lake** (no copia datos) |
| 3. BigQuery | `BigQueryInsertJobOperator` | `CREATE TABLE AS SELECT` desde BigLake → **Data Warehouse** particionado y clustered |

**Ventajas sobre Dataflow:**
- Los load jobs de BigQuery son **gratuitos** (no hay costo de procesamiento)
- Los datos raw quedan **una sola vez** en GCS; BigQuery solo almacena la tabla materializada
- Menos infraestructura que gestionar (no requiere habilitar la API de Dataflow)
- Cumple el mismo objetivo: Data Lake separado del Data Warehouse con transformación orquestada

**Datos subidos a GCS (optimización de costos):**
- `devices/` → `part-0000.parquet` (1 archivo, tabla de dimensión completa — 2.1M filas)
- `samples/` → `part-0000.parquet` a `part-0009.parquet` (10 archivos ≈ 21M filas)

**Flujo del DAG:**
```
upload_to_gcs  ──►  create_biglake_table  ──►  load_to_bigquery
  (local→GCS)          (tabla externa           (CTAS particionada
  11 Parquets)          = Data Lake)              = Data Warehouse)
```

---

## Paso 7 — Infraestructura Cloud con Terraform

Recursos creados con Terraform en el proyecto GCP `kestra-sandbox-2026`:

- **Google Cloud Storage bucket** `greenhub-raw-kestra-2026` — Data Lake (región: `us-central1`, borrado automático a los 90 días)
- **BigQuery dataset** `greenhub` — Data Warehouse (región: `us-central1`)

```bash
cd terraform
terraform init   # descarga hashicorp/google v5.45.2
terraform plan   # preview: +2 to add
terraform apply  # escribir "yes" para confirmar
```

**Output del apply:**

```
google_bigquery_dataset.greenhub: Creation complete after 1s  [id=projects/kestra-sandbox-2026/datasets/greenhub]
google_storage_bucket.raw:       Creation complete after 2s  [id=greenhub-raw-kestra-2026]

Apply complete! Resources: 2 added, 0 changed, 0 destroyed.

Outputs:
  bq_dataset          = "greenhub"
  bq_dataset_location = "us-central1"
  bucket_name         = "greenhub-raw-kestra-2026"
  bucket_url          = "gs://greenhub-raw-kestra-2026"
```

**¿Qué hace cada comando?**

| Comando | Descripción | Toca GCP |
|---|---|---|
| `terraform init` | Descarga el provider `hashicorp/google` y crea `.terraform.lock.hcl` con la versión exacta. Inicializa el backend local (`terraform.tfstate`). | ❌ No |
| `terraform plan` | Muestra un preview de los recursos a crear/modificar/destruir sin aplicar nada. | ❌ No |
| `terraform apply` | Crea los recursos reales en GCP (bucket GCS + dataset BigQuery). Pide confirmación antes de ejecutar. | ✅ Sí |

---

## Paso 8 — Data Warehouse: Schema SQL

### PostgreSQL (local)

```sql
-- Tabla particionada por rango de timestamp
-- Índice compuesto en battery_state + timestamp
CREATE TABLE samples (
    timestamp       TIMESTAMPTZ NOT NULL,
    device_id       TEXT NOT NULL,
    battery_state   TEXT,
    charger         TEXT,
    battery_level   FLOAT,
    cpu_usage       FLOAT,
    memory_free     FLOAT,
    memory_used     FLOAT
) PARTITION BY RANGE (timestamp);

CREATE INDEX idx_battery_state ON samples (battery_state, timestamp);
```

### BigQuery (cloud)

```sql
CREATE TABLE `kestra-sandbox-2026.greenhub.samples`
PARTITION BY DATE(timestamp)
CLUSTER BY battery_state, charger;
```

### Tabla Externa (BigLake) vs Tabla Nativa

El pipeline implementa el patrón **Data Lake → Data Warehouse** usando ambos tipos de tabla:

```
GCS (Parquet)
  └─► devices_external / samples_external   ← BigLake: capa cruda, barata, siempre actualizada
            └─► CTAS
                  └─► devices / samples     ← Nativa: capa de análisis, rápida, particionada
```

| Característica | Tabla Externa (BigLake) | Tabla Nativa |
|---|---|---|
| Dónde viven los datos | GCS (Parquet en el bucket) | BigQuery storage interno (Capacitor) |
| Costo de storage | Solo pagas GCS (~$0.02/GB/mes) | ~$0.02/GB/mes (similar) |
| Velocidad de consulta | Más lenta — lee desde GCS en cada query | Mucho más rápida — datos columnarizados y comprimidos |
| Particionado / Clustering | ❌ No disponible | ✅ Sí — reduce bytes escaneados |
| DML (`INSERT`, `UPDATE`) | ❌ No | ✅ Sí |
| Uso típico | Staging, datos que cambian en origen | Dashboard, queries analíticas |

**Por qué usar las dos:** la tabla externa es el punto de entrada barato y siempre sincronizado con GCS. La tabla nativa es una copia optimizada sobre la que se construyen los dashboards — gracias al particionado por `DATE(timestamp)` y el clustering por `battery_state, charger`, BigQuery solo escanea las particiones necesarias reduciendo costo y latencia.

---

## Paso 9 — Transformaciones

- **Local:** SparkSQL dentro del DAG de Airflow (modo `local[*]`)
- **Cloud:** BigQuery CTAS orquestado por Airflow (GCS BigLake → BigQuery nativo)

---

## Paso 10 — Dashboard

**Herramienta:** Looker Studio

**2 visualizaciones requeridas:**
1. Distribución del estado de batería por tipo de cargador (gráfico de barras apiladas — categórico)
2. Evolución del nivel de batería promedio en el tiempo (gráfico de líneas — temporal)

### Estrategia: Vistas Pre-Agregadas vs Conexión Directa

Looker Studio puede conectarse a BigQuery de dos maneras. En este proyecto usamos **vistas pre-agregadas** para optimizar velocidad y costo:

| | Conexión directa a `samples` | Vista pre-agregada |
|---|---|---|
| Qué ocurre en cada refresh | BigQuery escanea ~21M filas + JOIN 2.1M filas | BigQuery lee solo las filas ya agrupadas (decenas) |
| Velocidad | 2–5 segundos por tile | < 1 segundo |
| Costo por refresh | ~$0.005 | ~$0.0001 |
| Filtros interactivos | ✅ Cualquier columna | ✅ Solo las dimensiones incluidas en la vista |
| Cuándo conviene | Tablas < 1M filas o exploración ad-hoc | Dashboards con muchas visitas y > 10M filas |

Las vistas incluyen la columna `date` para que Looker Studio pueda ofrecer un **filtro de rango de fechas interactivo** — el visitante puede acotar el período que quiere analizar sin necesidad de correr queries sobre la tabla completa.

**Vistas creadas en BigQuery:**

```sql
-- Tile 1: distribución de estado de batería por cargador y fecha
-- queries/views/v_battery_by_charger_daily.sql

-- Tile 2: evolución del nivel de batería promedio diario
-- queries/views/v_battery_level_daily.sql
```

### Fuentes de datos en Looker Studio

| Tile | Fuente BigQuery | Tipo de gráfico |
|---|---|---|
| Estado de batería por cargador | `greenhub.v_battery_by_charger_daily` | Barras apiladas (100%) |
| Evolución nivel de batería | `greenhub.v_battery_level_daily` | Líneas temporales |

**Filtro global:** `date` (rango de fechas) — aplica a ambos tiles simultáneamente.

### Dashboard Público

🔗 **[Ver dashboard en Looker Studio](https://lookerstudio.google.com/reporting/2bba7aae-c1ed-4d00-be9f-eea7044b93e8)**

**Bonus:** Streamlit como alternativa open-source

---

## Reproducibilidad: Cómo Ejecutar el Proyecto

### Opción A — Local

```bash
# 1. Clonar el repositorio
git clone https://github.com/ayoquiroga/greenhub-farmer.git
cd greenhub-farmer

# 2. Configurar variables de entorno
cp .env.example .env
# Editar .env con tus credenciales de Kaggle y PostgreSQL

# 3. Levantar todos los contenedores
docker compose up -d

# 4. Acceder a Airflow
# → http://localhost:8080  (user: airflow / pass: airflow)

# 5. Activar el DAG "greenhub_local_pipeline"

# 6. Verificar datos en pgAdmin
# → http://localhost:8085  (email: admin@admin.com / pass: root)
```

### Opción B — Cloud (GCP)

```bash
# 1. Configurar credenciales GCP
export GOOGLE_APPLICATION_CREDENTIALS="path/to/service-account.json"

# 2. Provisionar infraestructura con Terraform
cd terraform
terraform init
terraform plan
terraform apply

# 3. Levantar Airflow
docker compose up -d

# 4. Activar el DAG "greenhub_gcp_pipeline"
```

---

## Evaluación del Proyecto (Criterios del Curso)

| Criterio | Estado | Descripción |
|---|---|---|
| Descripción del problema | ✅ | Dataset, objetivo y modelo de datos documentados |
| Cloud + IaC (Terraform) | ✅ | GCP con Terraform (BigQuery + GCS) |
| Ingesta por lotes (DAG) | ✅ | Airflow 2.9 + Docker Compose, DAGs completados |
| Data Warehouse (particionado) | ✅ | `PARTITION BY timestamp`, `CLUSTER BY battery_state` |
| Transformaciones | ✅ | SparkSQL (local) y BigQuery CTAS via BigLake (cloud) |
| Dashboard (2 tiles) | ✅ | [Looker Studio — 2 visualizaciones](https://lookerstudio.google.com/reporting/2bba7aae-c1ed-4d00-be9f-eea7044b93e8) |
| Reproducibilidad | ✅ | `docker compose up`, Terraform, instrucciones en README |

---

## Tips y Lecciones Aprendidas

### Docker

**Instrucción `RUN`: cada una crea una capa read-only**

Cada instrucción en un Dockerfile (`FROM`, `RUN`, `COPY`, `ENV`, etc.) agrega una capa inmutable encima de la anterior. Al correr el contenedor, Docker añade automáticamente una única capa **escribible** encima de todas las read-only — todo lo que el proceso escribe ahí desaparece cuando el contenedor se elimina (salvo que uses volúmenes).

```
┌─────────────────────────────┐  ← capa escritura (solo al correr, efímera)
├─────────────────────────────┤
│  RUN pip install ...        │  ← read-only
├─────────────────────────────┤
│  COPY requirements.txt ...  │  ← read-only
├─────────────────────────────┤
│  RUN apt-get install java   │  ← read-only
├─────────────────────────────┤
│  FROM apache/airflow:2.9.0  │  ← read-only (imagen base)
└─────────────────────────────┘
```

**Encadenar comandos en un solo `RUN` para mantener la imagen compacta**

Si `apt-get clean` y `rm -rf /var/lib/apt/lists/*` estuvieran en `RUN` separados, los archivos temporales quedarían guardados en la capa anterior y el tamaño final de la imagen no se reduciría. Al hacerlo todo en un único `RUN` con `&&`, la capa resultante ya no contiene esos archivos:

```dockerfile
# ✅ Correcto — una sola capa, imagen limpia
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jdk-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# ❌ Incorrecto — los temporales quedan guardados en la capa del install
RUN apt-get update
RUN apt-get install -y openjdk-17-jdk-headless
RUN apt-get clean && rm -rf /var/lib/apt/lists/*
```

---

### Airflow

**`schedule_interval="@once"` no es un schedule recurrente**

Un DAG con `@once` se ejecuta **una única vez** cuando se activa por primera vez o cuando se lanza un Trigger manual. No corre automáticamente de forma periódica. Para dispararlo hay que ir a la UI (http://localhost:8080) y hacer **▶ Trigger DAG**.

**`airflow tasks run` vs `airflow tasks test`**

- `airflow tasks run` requiere conectarse a la base de datos de metadatos de Airflow (DNS interno). Si se ejecuta desde fuera del contexto del scheduler, falla con un error de resolución DNS.
- `airflow tasks test` ejecuta la tarea directamente **sin escribir en la base de metadatos**, lo que lo hace útil para re-ejecutar una tarea puntual sin depender del estado del scheduler.

```bash
# ✅ Para re-ejecutar una tarea sin tocar el estado del DAG
airflow tasks test <dag_id> <task_id> '<execution_date>'
```

---

### Compatibilidad de dependencias Python en Airflow 2.9

Airflow 2.9 usa **SQLAlchemy 1.4.x** internamente, lo que impone restricciones en cadena:

| Paquete | Versión correcta | Motivo |
|---|---|---|
| `pandas` | `==2.1.4` | pandas 3.x requiere SQLAlchemy 2.x (incompatible). `apache-airflow-providers-google` requiere `pandas<2.2` |
| `kaggle` | `==1.6.17` | `1.7.x` tiene un bug de `FileExistsError` al importar, y un error de protobuf (`DatasetInfo has no "info" field`) |
| `pyarrow` | `>=23.0.0` | Necesario para `iter_batches()` en la carga de Parquet |

---

### Carga de datasets grandes en PostgreSQL — evitar OOM

Cargar archivos Parquet completos (~700K filas × 42 columnas) en un DataFrame de pandas antes de insertarlos genera picos de memoria que terminan en SIGKILL (exit code -9). La solución es usar micro-batches con PyArrow:

```python
# ❌ Causa OOM con archivos grandes
df = pd.read_parquet(part_file)
df.to_sql("samples", engine, if_exists="append")

# ✅ Micro-batches de 50K filas: memoria constante
import pyarrow.parquet as pq
import gc

pf = pq.ParquetFile(part_file)
for batch in pf.iter_batches(batch_size=50_000):
    batch_df = batch.to_pandas()
    batch_df.to_sql("samples", engine, if_exists="append", chunksize=5000)
    del batch_df
    gc.collect()
```

**Monitorear el progreso de carga desde la terminal**

La tarea `postgres_load` puede tardar horas (22 archivos × ~2.1M filas cada uno ≈ 49M filas totales). Para ver en tiempo real cuántos archivos y filas se han cargado, seguir el log de Airflow con `tail -f`:

```bash
docker exec greenhub-airflow bash -c \
  "tail -f '/opt/airflow/logs/dag_id=greenhub_local_pipeline/run_id=manual__<EXECUTION_DATE>/task_id=postgres_load/attempt=1.log'" \
  2>&1
```

> Reemplazar `<EXECUTION_DATE>` con el `run_id` real. Si no lo tenés a mano desde la UI de Airflow, podés listarlo directamente desde la terminal:
>
> ```bash
> docker exec greenhub-airflow bash -c "ls -lh '/opt/airflow/logs/dag_id=greenhub_local_pipeline/'" 2>&1
> ```
>
> Ejemplo de salida:
>
> ```
> /opt/airflow/logs/dag_id=greenhub_local_pipeline/:
> run_id=manual__2026-03-27T18:31:05.779561+00:00
> run_id=scheduled__2026-01-01T00:00:00+00:00
> ```
>
> Usar el `run_id` con el timestamp más reciente (el `manual__...` si fue disparado manualmente).

Ejemplo de salida esperada (una línea cada ~9 minutos aproximadamente):

```
[2026-03-27T19:04:28.021+0000] {logging_mixin.py:188} INFO -   [1/22] → 2,112,699 filas cargadas...
[2026-03-27T19:13:27.900+0000] {logging_mixin.py:188} INFO -   [2/22] → 4,225,398 filas cargadas...
[2026-03-27T19:21:56.475+0000] {logging_mixin.py:188} INFO -   [3/22] → 6,338,097 filas cargadas...
[2026-03-27T19:31:48.456+0000] {logging_mixin.py:188} INFO -   [4/22] → 8,450,796 filas cargadas...
[2026-03-27T19:41:02.744+0000] {logging_mixin.py:188} INFO -   [5/22] → 10,563,495 filas cargadas...
[2026-03-27T19:50:06.620+0000] {logging_mixin.py:188} INFO -   [6/22] → 12,676,194 filas cargadas...
[2026-03-27T19:58:42.604+0000] {logging_mixin.py:188} INFO -   [7/22] → 14,788,893 filas cargadas...
[2026-03-27T20:07:16.239+0000] {logging_mixin.py:188} INFO -   [8/22] → 16,901,592 filas cargadas...
[2026-03-27T20:15:34.308+0000] {logging_mixin.py:188} INFO -   [9/22] → 19,014,291 filas cargadas...
[2026-03-27T20:24:53.130+0000] {logging_mixin.py:188} INFO -  [10/22] → 21,126,990 filas cargadas...
[2026-03-27T20:36:21.427+0000] {logging_mixin.py:188} INFO -  [11/22] → 23,239,689 filas cargadas...
```

