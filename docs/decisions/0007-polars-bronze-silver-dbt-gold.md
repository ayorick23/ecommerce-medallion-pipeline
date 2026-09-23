# 0007 — Motor de transformación: Polars en Bronze/Silver, dbt (dbt-duckdb) en Gold

**Estado:** Aceptada
**Fecha:** 2026-09-23
**Supera parcialmente a:** [0002](0002-desacople-logica-transformacion-airflow.md)
(solo en lo que respecta a la capa Gold)

## Contexto

La ADR 0002 estableció que Bronze/Silver/Gold se implementan como funciones
Python puras en `src/`, desacopladas de Airflow. Al revisar el stack antes
de la Fase 2 se evaluó dónde encaja **dbt** (data build tool): la
herramienta estándar para la "T" de ELT dentro de un warehouse (modelos
SQL con dependencias vía `ref()`, tests declarativos, documentación y
linaje autogenerados). dbt Core es open-source y tiene adaptador para
DuckDB (`dbt-duckdb`).

No toda la lógica del pipeline tiene la misma forma:

- **Bronze**: simular el replay diario, re-emitir filas por evento (ADR
  0004), escribir particiones con linaje — lógica procedural sobre
  archivos, no un `SELECT`.
- **Silver**: deduplicación, unpivot para el SCD2, normalización y
  validación fail-fast con Pandera — ya diseñado sobre DataFrames.
- **Gold**: dimensiones y hechos a partir de joins sobre Silver — el
  terreno natural del modelado dimensional en SQL.

## Decisión

- **Bronze y Silver**: funciones Python puras con **Polars** en `src/`,
  validación con **Pandera** (sin cambios respecto de la ADR 0002).
- **Gold**: proyecto **dbt Core + `dbt-duckdb`**. Los modelos leen Silver
  (Parquet) como *sources* y materializan `dim_*` / `fct_*` como tablas en
  `warehouse.duckdb` (ADR 0005). La validación Silver→Gold (reglas de la
  sección 4 de `docs/schemas.md`) se implementa como **tests de dbt**
  (`not_null`, `unique`, `relationships`, rangos), en lugar de schemas
  Pandera.
- El principio de fondo de la ADR 0002 se mantiene: la lógica no vive en
  Airflow. El DAG solo invoca `dbt build` como una task más.

## Alternativas consideradas

- **Todo en Polars** (plan original): coherente y con menos piezas, pero
  deja fuera la herramienta de transformación más extendida del mercado y
  resuelve con Python algo que se expresa mejor en SQL.
- **dbt en Silver y Gold**: el replay incremental, el SCD2 derivado de
  re-emisiones y la validación con Pandera ya están diseñados sobre
  DataFrames; forzarlos a SQL no aporta y desperdicia el diseño de la
  Fase 1.
- **SQL de DuckDB "a mano"** (scripts `.sql` ejecutados desde Python): da
  el SQL pero no el grafo de dependencias, los tests ni la documentación
  que hacen de dbt el estándar.

## Por qué

Es un patrón híbrido real de la industria: Python donde hay lógica
procedural, SQL/dbt donde hay modelado. Dentro del portafolio, dbt es
terreno nuevo (el Proyecto 1 cubre SQL analítico y Power BI; el 2, MLOps)
y es la habilidad que separa "saber SQL" de "modelar datos en producción".
Solo afecta a la Fase 4, así que no se rehace nada ya diseñado.

## Consecuencias

- Fase 4 cambia de entregable: proyecto dbt (modelos, `sources`, tests,
  docs) en vez de funciones Polars + Pandera para Gold. Ubicación exacta
  del proyecto dbt en el repo: se decide al diseñar la Fase 4.
- Las reglas fail-fast Silver→Gold siguen siendo las de `docs/schemas.md`;
  cambia la herramienta, no la regla. Un test de dbt fallido hace fallar
  `dbt build` → falla la task → se detiene el DAG (hard-stop, ADR 0001).
- Algunas reglas (grano compuesto único, rangos de medidas) requieren el
  paquete `dbt_utils` o tests genéricos propios.
- Fase 5: dbt y Airflow tienen historial de conflictos de dependencias al
  instalarse en el mismo entorno. Opciones a evaluar entonces: entorno
  virtual aislado para dbt dentro de la imagen, o `astronomer-cosmos`.
- `dbt build` escribe en `warehouse.duckdb`: sujeto a la restricción de
  un solo escritor (ADR 0005).
- `tests/unit/gold/` pierde sentido como carpeta de pytest; los tests de
  Gold viven en el proyecto dbt. Se revisa en la Fase 4/6.
