# ecommerce-medallion-pipeline

Pipeline de Data Engineering para e-commerce (arquitectura medallion:
Bronze/Silver/Gold) con Polars, Airflow y DuckDB, sobre el dataset
[Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).

> Proyecto de portafolio en construcción por fases. El plan completo y las
> decisiones de arquitectura viven en [`docs/decisions/plan-fases.md`](docs/decisions/plan-fases.md)
> y [`docs/decisions/`](docs/decisions/).

## Quickstart

Requisitos: [`uv`](https://docs.astral.sh/uv/) y Docker.

```bash
uv sync --all-groups          # instala dependencias (Python 3.12)
cp .env.example .env          # credenciales del admin de Airflow

docker compose up             # Airflow (LocalExecutor) + Postgres
# UI en http://localhost:8080
```

Dataset crudo: descargar los CSVs de Olist en `data/raw/` (no se versiona,
ver `.gitignore`). La exploración inicial está en
[`notebooks/01_exploracion_olist.ipynb`](notebooks/01_exploracion_olist.ipynb).

## Desarrollo

```bash
uv run pytest          # tests
uv run ruff check .    # lint
uv run ruff format .   # formateo
uv run mypy src        # type-check
```

`pre-commit install` corre estas mismas verificaciones antes de cada commit
(config en `.pre-commit-config.yaml`); un workflow de GitHub Actions las
replica en cada push (`.github/workflows/ci.yml`).

## Estructura

```
src/          lógica de transformación (funciones puras, sin Airflow)
dags/         DAGs de Airflow (envoltorios delgados sobre src/)
config/       configuración externalizada (YAML/.env)
tests/        unit/ (espejo de src/) e integration/
notebooks/    exploración, no forma parte del pipeline
docs/         plan por fases y ADRs
```

Convenciones de trabajo (git, calidad de código, ADRs) en
[`CLAUDE.md`](CLAUDE.md).
