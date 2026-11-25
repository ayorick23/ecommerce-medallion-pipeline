# Airflow con el pipeline instalado en un venv aparte (ADR 0041).
#
# Airflow vive en su entorno (el de la imagen oficial) y el pipeline en
# /opt/medallion/.venv, con exactamente las versiones de uv.lock: lo mismo que
# probó la CI. El DAG no importa medallion; lanza ese venv como proceso.
FROM apache/airflow:2.10.4-python3.12

COPY --from=ghcr.io/astral-sh/uv:0.12.3 /uv /usr/local/bin/uv

USER root
# git: dbt instala dbt-utils desde su repo, no desde el hub (ADR 0036).
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /opt/medallion && chown airflow:0 /opt/medallion
USER airflow
WORKDIR /opt/medallion

# Python de la imagen (sin descargar otro), venv en la raíz del pipeline y
# bytecode compilado para que cada task no lo compile al arrancar.
ENV UV_PROJECT_ENVIRONMENT=/opt/medallion/.venv \
    UV_PYTHON=/usr/local/bin/python3.12 \
    UV_PYTHON_DOWNLOADS=never \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_NO_CACHE=1

# Primero solo las dependencias: esta capa se reutiliza mientras no cambie el lock.
# --locked falla si uv.lock no está al día con pyproject.toml, en vez de resolver.
COPY --chown=airflow:0 pyproject.toml uv.lock .python-version README.md ./
RUN uv sync --locked --no-dev --group gold --no-install-project

COPY --chown=airflow:0 src ./src
RUN uv sync --locked --no-dev --group gold --no-editable

# dbt corre desde /opt/medallion (DBT_PROJECT_DIR es relativo). config/ se copia
# solo para `gold-dbt deps`; en ejecución, el compose monta el del repo encima.
COPY --chown=airflow:0 dbt ./dbt
COPY --chown=airflow:0 config ./config
RUN .venv/bin/gold-dbt deps

# El contenedor puede correr con otro UID (AIRFLOW_UID) y grupo 0: dbt escribe
# en dbt/target y dbt/logs, así que el grupo necesita escritura.
RUN chmod -R g+rwX /opt/medallion

ENV PIPELINE_HOME=/opt/medallion \
    PIPELINE_CONFIG=/opt/medallion/config/pipeline.yaml

WORKDIR /opt/airflow
