# =========================================================
# Imagen personalizada de Airflow para GreenHub Farmer
#
# Base:   apache/airflow:2.9.0 (Debian 11 Bullseye)
# Extras: Java 17 (requerido por PySpark) + deps Python
# =========================================================

FROM apache/airflow:2.9.0

# ── Instalar Java como root ───────────────────────────────
USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jdk-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# ── Instalar dependencias Python como usuario airflow ────
USER airflow

# El contexto del build es la raíz del proyecto (context: .)
COPY docker/airflow/requirements.txt /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir -r /tmp/requirements-airflow.txt
