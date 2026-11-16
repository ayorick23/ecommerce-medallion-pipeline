---
paths:
  - "dbt/**"
  - "src/medallion/gold/**"
  - "tests/integration/gold/**"
---

# Gold con dbt: convenciones del proyecto

Se carga solo al trabajar con archivos de Gold (ADR 0038). El detalle y el
porqué de cada punto están en la ADR citada; aquí va lo que no hay que romper.

## Cómo se corre dbt

- **Nunca `dbt` a secas.** `profiles.yml` y los sources usan `env_var()` sin
  valor por defecto: `uv run gold-dbt <args>` para desarrollo y
  `uv run gold-build --dia D [--full-refresh]` para el pipeline (ADR 0027).
  dbt corre en un proceso hijo, no con `dbtRunner` (ADR 0037).
- `gold-dbt` escribe directo en `data/gold/warehouse.duckdb`, sin guardas.
  Para validar un cambio de verdad: `gold-build` o los tests de integración.
- No correr dos dbt a la vez: comparten `dbt/target/`.
- Después de un clon: `uv run gold-dbt deps` (`dbt_packages/` no se versiona).

## Capas (ADR 0026)

- **staging** (`stg_*`, vistas): solo renombra y castea desde
  `{{ source('silver', ...) }}`. Sin lógica, sin joins, sin tests unitarios.
- **intermediate** (`int_*`, vistas): la lógica real. Cada caso de diseño
  tiene un unit test de dbt con nombre en `_intermediate.yml` (ADR 0036).
- **marts** (`dim_*`, `fct_*`, `puente_*`): tablas con
  `contract: {enforced: true}`. Toda columna nueva va al `_marts.yml` con
  `data_type` y `description`; castea explícito (p. ej. montos a
  `decimal(18,2)` después de agregar, ADR 0022).

## Materialización (ADR 0029)

- Dimensiones: tabla completa. Hechos: `incremental` con
  `on_schema_change='fail'` y marca de agua en `_visible_desde`.
- El filtro incremental usa **siempre** `{{ marca_de_agua() }}`, nunca
  `max(_visible_desde)` directo: con la tabla vacía, `max()` es NULL y el
  hecho no vuelve a cargar nada.
- `_visible_desde` debe ser el instante en que la fila apareció o cambió
  por última vez en Silver(D), y nunca posterior a D.
- Todo cambio a un incremental se prueba con la equivalencia incremental ==
  full refresh: `uv run pytest tests/integration/gold` (~6 min).

## Modelado

- `dim_cliente` es SCD2 de persona con `cliente_sk` = hash de
  `(customer_unique_id, valid_from)`; los hechos apuntan a la versión vigente
  a su fecha (ADR 0030). Intervalos semiabiertos `[valid_from, valid_to)`.
- Estado del pedido: solo los 6 de `dim_estado_pedido` (ADR 0031).
- Comparar contra una fecha sin hora (p. ej. la entrega estimada, siempre a
  las 00:00) se hace por día calendario, no por timestamp (ADR 0039).
- Fechas que son promesas (`fecha_entrega_estimada`, `fecha_limite_envio`)
  son las únicas que pueden ser posteriores a D.
- `dim_tiempo` es un calendario fijo con los límites de `dbt_project.yml`
  (ADR 0032).

## Tests y estilo

- Tests de datos con severidad `error` (el valor por defecto); nunca `warn`
  (ADR 0001). `dbt_utils` para claves compuestas, rangos y SCD2; un test
  genérico propio solo si ningún paquete lo cubre (ADR 0036).
- SQL formateado con `uv run sqlfmt dbt` (pre-commit y CI lo verifican).
- Nombres de columnas y modelos en español, salvo los heredados de Olist.
