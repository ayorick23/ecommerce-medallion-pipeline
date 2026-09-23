# 0005 — Almacenamiento por capa: Parquet en Bronze/Silver, DuckDB en Gold

**Estado:** Aceptada
**Fecha:** 2026-09-23

## Contexto

Antes de implementar Bronze (Fase 2) hay que fijar dónde viven físicamente
los datos de cada capa y en qué formato entran y salen. El `docker-compose`
ya trae un Postgres, pero es el metastore interno de Airflow (estado de
DAGs, ejecuciones, tasks) — no está pensado para alojar los datos del
pipeline, y mezclar ambas cosas acoplaría los datos al ciclo de vida del
orquestador.

La carga de trabajo es analítica (OLAP): leer muchas filas de pocas
columnas para deduplicar, unir y agregar. No hay escrituras concurrentes
pequeñas como en una aplicación transaccional (OLTP).

## Decisión

| Capa | Entra | Sale | Ubicación |
| --- | --- | --- | --- |
| Raw | — | CSV originales de Olist, inmutables | `data/raw/` |
| Bronze | CSV | Parquet, particionado por `dia_simulado` (layout estilo Hive) en tablas de eventos; snapshot en tablas de referencia | `data/bronze/<tabla>/` |
| Silver | Parquet Bronze | Parquet, un dataset por tabla | `data/silver/<tabla>/` |
| Gold | Parquet Silver | Tablas `dim_*` / `fct_*` en un archivo DuckDB | `data/gold/warehouse.duckdb` |

Postgres queda exclusivamente como metastore de Airflow. Ningún dato de
Olist se escribe ahí.

Las rutas concretas son relativas a una raíz configurable (ADR 0008).

## Alternativas consideradas

- **Todo en Postgres** (usar el mismo servidor del metastore o uno aparte):
  motor orientado a filas, pensado para OLTP; más lento y más pesado para
  este tipo de consultas, y no es el patrón de una arquitectura medallion.
- **Todo en DuckDB** (las tres capas como tablas en un solo archivo):
  simple, pero pierde el particionado físico por día que hace visible y
  verificable la ingesta incremental de Bronze, y concentra todo en un
  archivo con un único escritor.
- **CSV en Bronze/Silver**: sin tipos, sin compresión, sin schema embebido
  — cada lectura vuelve a inferir tipos.

## Por qué

Es el patrón estándar de un lakehouse: archivos columnares (Parquet) en
almacenamiento de objetos para las capas intermedias, y un motor SQL
analítico para la capa de consumo. Parquet guarda el schema y los tipos
dentro del archivo, comprime bien y lo leen Polars, DuckDB, Spark y
cualquier warehouse. DuckDB cumple el papel de Snowflake/BigQuery/Synapse
en local: OLAP columnar, embebido, sin servidor, gratis.

## Consecuencias

- DuckDB admite **un solo proceso escritor por archivo**: las escrituras a
  `warehouse.duckdb` deben serializarse en el DAG (Fase 5).
- Gold es consultable con SQL directamente desde un notebook, la CLI de
  DuckDB o un BI, sin levantar ningún servicio.
- `data/` sigue fuera de git (ver `.gitignore`); todo lo que hay debajo de
  `raw/` se puede regenerar corriendo el pipeline.
