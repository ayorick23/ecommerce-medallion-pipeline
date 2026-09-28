# 0026 — Proyecto dbt de Gold: ubicación, capas y dependencias

**Estado:** Aceptada
**Fecha:** 2026-09-28

## Contexto

La ADR 0007 fijó dbt Core + `dbt-duckdb` para Gold y dejó para esta fase
tres cosas: dónde vive el proyecto dbt dentro del repo, cómo se organizan
sus modelos y cómo se instalan sus dependencias sin mezclarlas con las del
resto del pipeline. dbt tiene además un historial de conflictos de
dependencias con Airflow (Fase 5), así que conviene que su instalación sea
separable desde el principio.

## Decisión

- **Ubicación:** el proyecto dbt vive en la carpeta `dbt/`, en la raíz del
  repo (hermana de `src/`, `config/` y `tests/`):

  ```text
  dbt/
  ├── dbt_project.yml
  ├── profiles.yml        ← versionado: sin secretos, solo env_var (ADR 0027)
  ├── packages.yml        ← dbt_utils con versión exacta (ADR 0036)
  ├── models/
  │   ├── staging/        ← stg_<tabla>: un modelo por tabla de Silver
  │   ├── intermediate/   ← int_<concepto>: lógica que no es un mart
  │   └── marts/          ← dim_*, fct_*, puente_*
  ├── seeds/              ← dim_estado_pedido, feriados (ADR 0031, 0032)
  └── tests/              ← tests singulares, si hacen falta
  ```

- **Capas:**
  - `staging`: vistas. Renombran y castean las columnas de Silver, sin
    lógica de negocio. Una por tabla fuente.
  - `intermediate`: vistas con trabajo real, no de relleno. De entrada,
    `int_cliente_direcciones` (versiones de dirección por persona, ADR
    0030) e `int_pedido_estado` (estado combinado del pedido, ADR 0031).
  - `marts`: la capa de consumo. Dimensiones como tablas y hechos como
    incrementales (ADR 0029).
  - Cada capa en su propio esquema de DuckDB; los consumidores usan solo el
    de marts.
- **Dependencias:** `dbt-core` y `dbt-duckdb` en un grupo de dependencias
  `gold` de `pyproject.toml`, separado de `dev` y de las dependencias del
  paquete. Versiones verificadas al diseñar: `dbt-core` 1.12.5 y
  `dbt-duckdb` 1.11.0, que conviven con `duckdb` 1.5.5 sin cambiarlo.

## Alternativas consideradas

- **Proyecto dbt dentro de `src/medallion/gold/`:** mezcla SQL y YAML de
  dbt con el paquete Python instalable, y dbt espera ser la raíz de su
  propio proyecto.
- **Sin capa intermediate** (staging → marts): válido en proyectos chicos,
  pero la construcción del SCD2 de clientes y el estado combinado quedarían
  enterrados dentro de los marts, sin poder probarse por separado (los unit
  tests de la ADR 0036 apuntan justo a esos dos modelos).
- **Intermediate como relleno** (un modelo por mart aunque no haga nada):
  cumple la forma y no el propósito. Un modelo intermedio existe solo si
  concentra lógica.
- **dbt en las dependencias del paquete:** obligaría a instalar dbt para
  correr Bronze o Silver, y a arrastrarlo al entorno de Airflow.

## Por qué

Es la estructura estándar de un proyecto dbt (la que recomienda dbt Labs),
así que cualquiera que conozca dbt se orienta sin leer documentación. El
grupo `gold` deja que la Fase 5 decida si dbt corre en un entorno aislado
sin tener que desenredar dependencias.

## Consecuencias

- `uv sync` instala solo el grupo `dev` por defecto: se agrega `gold` a
  `default-groups` en `[tool.uv]` para que `uv run gold-build` funcione en
  local. La CI ya instala todos los grupos (`uv sync --all-groups`).
- Staging e intermediate son vistas: guardan en `warehouse.duckdb` la ruta
  de los Parquet de Silver. Esa ruta tiene que ser absoluta (ADR 0027), y
  esas vistas leen el Silver *actual*, no necesariamente el de la fecha de
  Gold. Por eso no son parte del contrato de consumo: solo los marts lo son.
- `dbt/target/`, `dbt/logs/` y `dbt/dbt_packages/` van al `.gitignore`.
