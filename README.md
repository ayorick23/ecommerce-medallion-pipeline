# E-commerce Medallion Pipeline: Pipeline de Data Engineering con arquitectura Bronze/Silver/Gold

[![CI](https://github.com/ayorick23/ecommerce-medallion-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/ayorick23/ecommerce-medallion-pipeline/actions/workflows/ci.yml)
[![Python 3.12](https://img.shields.io/badge/python-3.12-blue)](https://www.python.org)
[![uv](https://img.shields.io/badge/managed%20with-uv-de5fe9)](https://docs.astral.sh/uv/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)

> 🚧 **Proyecto en desarrollo activo.** Este README se actualiza al cierre de cada fase del roadmap.

Pipeline de Data Engineering end-to-end sobre el dataset público de e-commerce de [Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) (~100 mil pedidos reales de Brasil, 2016-2018). Simula que dos años de datos llegan **día a día**, como en un sistema real, y los procesa en tres capas: **Bronze** (copia fiel y trazable de la fuente), **Silver** (datos validados y limpios) y **Gold** (modelo dimensional listo para análisis).

El objetivo no es solo mover datos de un lado a otro, sino demostrar las prácticas que hacen confiable a un pipeline en producción: reprocesos idempotentes, linaje a nivel de fila, validación que detiene el pipeline ante datos rotos, modelado dimensional y cada decisión de diseño documentada como ADR.

## Tabla de contenidos

- [E-commerce Medallion Pipeline: Pipeline de Data Engineering con arquitectura Bronze/Silver/Gold](#e-commerce-medallion-pipeline-pipeline-de-data-engineering-con-arquitectura-bronzesilvergold)
  - [Tabla de contenidos](#tabla-de-contenidos)
  - [Problema de negocio](#problema-de-negocio)
  - [Qué hace el pipeline](#qué-hace-el-pipeline)
  - [Qué no hace (por diseño)](#qué-no-hace-por-diseño)
  - [Arquitectura](#arquitectura)
  - [Stack tecnológico](#stack-tecnológico)
  - [Estructura del repositorio](#estructura-del-repositorio)
  - [Estado del proyecto](#estado-del-proyecto)
  - [Cómo ejecutarlo](#cómo-ejecutarlo)
    - [Replay de Bronze](#replay-de-bronze)
    - [Construir Silver](#construir-silver)
    - [Tests y calidad de código](#tests-y-calidad-de-código)
    - [Airflow](#airflow)
  - [Roadmap y decisiones de arquitectura](#roadmap-y-decisiones-de-arquitectura)
  - [Fuentes de datos y créditos](#fuentes-de-datos-y-créditos)
  - [Licencia](#licencia)

## Problema de negocio

Un e-commerce recibe cada día pedidos, pagos y reseñas, y cada pedido cambia de estado con el tiempo (comprado, aprobado, despachado, entregado). El equipo de análisis necesita responder preguntas de ventas, tiempos de entrega y satisfacción sobre un modelo **confiable**. Eso exige un pipeline que:

1. procese cada día de forma incremental, sin duplicar lo ya cargado;
2. se pueda **reprocesar** cualquier día (una corrección en la fuente, un bug arreglado) y obtener el mismo resultado;
3. se **detenga** ante datos estructuralmente rotos, en lugar de propagarlos a los reportes;
4. permita rastrear cualquier número de un reporte hasta el archivo fuente y la corrida que lo produjo.

## Qué hace el pipeline

- ✅ **Simula la llegada diaria de los datos:** un replay recorre los 774 días del dataset; cada pedido "llega" de nuevo cada día en que cambia de estado (ADR 0004).
- ✅ **Bronze:** guarda por día una copia fiel de lo que llegó (todo como texto, sin corregir nada), particionada por `dia_simulado`, con linaje por fila (`ingested_at`, `source_file`, `batch_hash`). Reprocesar un día reemplaza su partición de forma atómica: nunca duplica ni deja archivos a medio escribir.
- ✅ **Silver:** reconstruye "a la fecha D" una foto limpia y tipada de todo lo que se sabía al cierre de ese día, **sin filtrar información del futuro** (ADR 0017, 0018). Incluye el historial de estados de cada pedido como SCD2 (una versión vigente por instante, aun con timestamps de la fuente fuera de orden, ADR 0019), montos exactos en `Decimal(18,2)` y reviews que llegan antes que su pedido esperando con un plazo de gracia (ADR 0020). Todo se valida con Pandera y chequeos de FK antes de escribir: ante cualquier violación, una sola `SilverValidationError` con todas las fallas y **nada escrito** (ADR 0023).
- 🔜 **Gold** (Fase 4): star schema (`fct_pedidos`, `fct_pagos`, `dim_*`) construido con dbt sobre DuckDB, con las reglas de calidad como tests de dbt.
- 🔜 **Orquestación** (Fase 5): DAGs de Airflow que envuelven las funciones de cada capa, con reintentos y manejo de fallos.

## Qué no hace (por diseño)

Decisiones deliberadas de alcance, no limitaciones por descuido:

- No procesa en streaming: la granularidad es un lote diario, que es lo que el caso de uso necesita.
- No corrige datos en Bronze: Bronze es la copia fiel de la fuente; limpiar y tipar es trabajo de Silver (ADR 0011).
- No actualiza Silver con merges incrementales: lo reconstruye completo a la fecha D, sin estado entre corridas (ADR 0017). El patrón incremental se aplica en Gold, con dbt.
- No sigue adelante "con warnings": una violación estructural detiene el pipeline (ADR 0001, 0003).
- No usa Delta Lake ni Iceberg: Parquet plano, con la idempotencia y la atomicidad garantizadas por el propio pipeline (ADR 0006, 0012, 0015).
- No versiona datos con DVC: la fuente es estática y cada capa se regenera con un replay (ADR 0010).
- No escribe en la nube por defecto: el almacenamiento es local; Azure Blob está contemplado como opción (ADR 0008).

## Arquitectura

```text
data/raw/*.csv  (Olist, inmutable)
   │  bronze-replay / Airflow → ingest_day(dia_simulado)     [Polars]
   ▼
data/bronze/<tabla>/dia_simulado=AAAA-MM-DD/part-0.parquet   (+ linaje)
   │  silver-build / Airflow → silver_build(D)               [Polars + Pandera, fail-fast]
   ▼
data/silver/<tabla>/part-0.parquet   (+ _manifest.json de la corrida)
   │  Airflow → dbt build                                     [dbt-duckdb, tests = fail-fast]
   ▼
data/gold/warehouse.duckdb  (star schema: dim_* / fct_*)
   │
   ▼
Consumo: SQL, notebooks, BI
```

La lógica de cada capa vive en funciones Python puras (paquete `medallion`) o en el proyecto dbt, **desacoplada de Airflow**: se testea con pytest en segundos, sin levantar un scheduler, y Airflow solo orquesta (ADR 0002, 0007).

## Stack tecnológico

| Categoría                            | Herramienta                                                      |
| ------------------------------------ | ---------------------------------------------------------------- |
| Transformación Bronze/Silver         | Polars                                                           |
| Validación Bronze→Silver             | Pandera                                                          |
| Transformación y calidad Silver→Gold | dbt Core + dbt-duckdb                                            |
| Almacenamiento                       | Parquet (Bronze/Silver), DuckDB (Gold)                           |
| Orquestación                         | Apache Airflow (Azure Data Factory documentado como alternativa) |
| Metastore de Airflow                 | Postgres                                                         |
| Testing                              | Pytest                                                           |
| Calidad de código                    | Ruff, MyPy (strict), pre-commit                                  |
| CI                                   | GitHub Actions                                                   |
| Empaquetado                          | uv                                                               |
| Contenedores                         | Docker, Docker Compose                                           |

## Estructura del repositorio

```text
ecommerce-medallion-pipeline/
├── src/medallion/
│   ├── common/            # configuración externalizada, escritura atómica
│   ├── bronze/            # ruteo por día, linaje, batch_hash, replay
│   ├── silver/            # tipos, SCD2, enmascarado a la fecha D, Pandera, silver-build
│   └── gold/              # (Fase 4)
├── tests/
│   ├── unit/              # espejo de las capas de src/medallion/
│   └── integration/       # flujos completos (CSV → Bronze → Silver) y propiedades por día
├── config/                # pipeline.yaml: almacenamiento, archivos fuente, plazo de gracia
├── dags/                  # DAGs de Airflow (Fase 5)
├── data/                  # raw/ bronze/ silver/ gold/ — fuera de git
├── notebooks/             # solo exploración (no forma parte del pipeline)
├── docs/
│   ├── schemas.md         # contratos de datos de las 3 capas
│   └── decisions/         # plan por fases y ADRs
├── .github/workflows/     # CI
├── docker-compose.yml     # Airflow (LocalExecutor) + Postgres
├── pyproject.toml
├── uv.lock
├── LICENSE
└── README.md
```

## Estado del proyecto

| Fase                                           | Estado         |
| ---------------------------------------------- | -------------- |
| 0. Fundamentos y entorno de trabajo            | ✅ Completado  |
| 1. Diseño de datos y contratos                 | ✅ Completado  |
| 2. Capa Bronze: ingesta y llegada incremental  | ✅ Completado  |
| 3. Validación y capa Silver                    | ✅ Completado  |
| 4. Capa Gold: modelado dimensional (dbt)       | 🔜 Siguiente   |
| 5. Orquestación con Airflow                    | ⏳ Pendiente   |
| 6. Testing automatizado                        | ⏳ Pendiente   |
| 7. Contenerización y reproducibilidad          | ⏳ Pendiente   |
| 8. Documentación y narrativa de portafolio     | ⏳ Pendiente   |

## Cómo ejecutarlo

Requiere [uv](https://docs.astral.sh/uv/) (instala Python 3.12 automáticamente) y, para Airflow, Docker.

```bash
# clonar el repo
git clone https://github.com/ayorick23/ecommerce-medallion-pipeline.git
cd ecommerce-medallion-pipeline

# instalar dependencias y el paquete del proyecto (medallion)
uv sync --all-groups
```

**Dataset:** descargar los 9 CSV de [Olist en Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) en `data/raw/`, con sus nombres originales (el mapeo tabla → archivo está en [`config/pipeline.yaml`](config/pipeline.yaml)). Los datos no se versionan.

### Replay de Bronze

```bash
# todo el rango de la fuente (2016-09-04 → 2018-10-17, ~12 min)
uv run bronze-replay

# un rango acotado
uv run bronze-replay --desde 2017-11-23 --hasta 2017-11-25

# con otra configuración
uv run bronze-replay --config ruta/a/pipeline.yaml
```

El replay es idempotente: se puede volver a correr cualquier rango y Bronze queda igual (salvo `ingested_at`). Imprime una línea por día con las filas escritas por tabla:

```text
2017-11-24  orders=1686 order_items=1366 order_payments=1214 order_reviews=270
```

La raíz de almacenamiento y el archivo de configuración también se pueden cambiar con las variables de entorno `PIPELINE_STORAGE_ROOT` y `PIPELINE_CONFIG`.

Para leer una tabla de Bronze (Polars reconstruye `dia_simulado` desde las carpetas):

```python
import polars as pl

orders = pl.read_parquet("data/bronze/orders", hive_partitioning=True)
```

**Bronze en números** (replay completo verificado contra la fuente): 774 días, 308,454 filas de `orders` (una por pedido y día con eventos), 112,650 de `order_items`, 103,886 de `order_payments`, 99,224 de `order_reviews` y 5 snapshots de referencia; 67.7 MB en Parquet frente a ~126 MB de CSV.

### Construir Silver

Con Bronze ya ingerido:

```bash
# Silver tal como se conocía al cierre de un día (el último del dataset, ~2 s)
uv run silver-build --dia 2018-10-17

# cualquier día intermedio: los eventos posteriores a esa fecha quedan ocultos
uv run silver-build --dia 2018-06-11 --config ruta/a/pipeline.yaml
```

Imprime las filas por tabla y las reviews que esperan a su pedido. Si algún dato viola el contrato, no escribe nada, termina con código 1 y lista todas las fallas:

```text
Silver no pasó la validación: 1 fallas; no se escribió nada.
- order_payments [payment_installments] medida_imposible: 1 filas (ej.: '-1')
```

Cada corrida deja `data/silver/_manifest.json` (fecha D, filas por tabla, días de Bronze leídos); si falta, Silver quedó incompleto y no se debe leer. Silver se lee directamente con Polars o DuckDB:

```python
import duckdb

duckdb.sql("select sum(payment_value) from 'data/silver/order_payments/part-0.parquet'")
```

**Silver en números** (último día, verificado contra la fuente): 99,441 pedidos, 392,856 eventos de estado en el SCD2, 112,650 items, 103,886 pagos, 99,224 reviews y 5 tablas de referencia; 31 MB, 0 fallas de validación, y reconstruir el mismo día da archivos idénticos byte a byte. Contratos completos en [`docs/schemas.md`](docs/schemas.md), secciones 3 a 5.

### Tests y calidad de código

```bash
uv run pytest                 # suite completa (unit + integration)
uv run ruff check .           # lint
uv run ruff format --check .  # formato
uv run mypy src tests         # type-check (strict)

uv run pre-commit install     # una vez por clon: corre ruff y mypy antes de cada commit
```

Los mismos checks corren en CI (`.github/workflows/ci.yml`) en cada push y en cada PR contra `main`.

### Airflow

```bash
cp .env.example .env          # credenciales del usuario admin de Airflow
docker compose up             # Airflow (LocalExecutor) + Postgres
# UI en http://localhost:8080
```

Por ahora levanta solo el entorno; los DAGs del pipeline llegan en la Fase 5.

## Roadmap y decisiones de arquitectura

El roadmap son las 9 fases de la tabla de "Estado del proyecto", detalladas en [`docs/decisions/plan-fases.md`](docs/decisions/plan-fases.md). Los contratos de datos de las tres capas están en [`docs/schemas.md`](docs/schemas.md), y cada decisión de diseño no trivial queda registrada como ADR en [`docs/decisions/`](docs/decisions/). Algunas de las más relevantes:

- **0004:** Bronze simula la llegada incremental re-emitiendo cada pedido el día de cada cambio de estado.
- **0011:** Bronze guarda todo como texto (*schema-on-read*); la conversión y validación de tipos es de Silver.
- **0012 y 0015:** reprocesar un día reemplaza su partición completa, con escritura atómica por archivo.
- **0014:** `batch_hash` con una serialización canónica sin ambigüedades (prefijo de longitud).
- **0016:** el código se instala como paquete (`medallion`), sin trucos de `PYTHONPATH`.
- **0017 y 0018:** Silver se reconstruye completo "a la fecha D" y oculta lo que todavía no ocurrió (sin *data leakage*).
- **0019:** SCD2 ordenado por etapa, con `valid_from` que nunca retrocede aunque la fuente traiga horas fuera de orden (1.4% de los pedidos).
- **0020:** reviews que llegan antes que su pedido esperan hasta 120 días; después, son huérfanas y detienen el pipeline.
- **0021:** el contrato de Silver se corrigió con datos medidos (reviews muchos a muchos, cuotas en 0, códigos postales con ceros a la izquierda).
- **0022:** montos en `Decimal(18,2)`, con un chequeo de formato que evita el truncado silencioso.

Las convenciones de trabajo del repositorio (git, calidad de código, ADRs) están en [`CLAUDE.md`](CLAUDE.md). La exploración inicial del dataset está en [`notebooks/01_exploracion_olist.ipynb`](notebooks/01_exploracion_olist.ipynb).

## Fuentes de datos y créditos

Este proyecto ha sido posible gracias a la disponibilidad de datos abiertos.

- **Dataset:** [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce), publicado en Kaggle por Olist bajo licencia [CC BY-NC-SA 4.0](https://creativecommons.org/licenses/by-nc-sa/4.0/) — ~100 mil pedidos reales de 2016 a 2018 en 9 tablas relacionadas (pedidos, items, pagos, reseñas, clientes, productos, vendedores, geolocalización y traducción de categorías), usadas como fuente cruda del pipeline (`data/raw/`, fuera de git: los datos no se redistribuyen).

Agradecemos a esta plataforma por facilitar el acceso a esta información para fines educativos y de portafolio.

## Licencia

Distribuido bajo licencia [MIT](LICENSE). El dataset conserva su propia licencia (ver sección anterior).

---

_Proyecto de portafolio personal — no reutiliza código ni datos de ningún proyecto profesional o propiedad de terceros._
