# 0032 — `dim_tiempo`: calendario fijo 2016–2020 con feriados de Brasil

**Estado:** Aceptada
**Fecha:** 2026-09-28

## Contexto

El contrato de la Fase 1 definía `dim_tiempo` cubriendo el rango del
dataset (2016-09 a 2018-10), con pocos atributos. Al diseñar Gold
aparecieron dos problemas:

- Hay fechas **legítimamente futuras**, que se conocen desde la compra: la
  entrega estimada y el plazo del vendedor (ADR 0018). Un
  `shipping_limit_date` llega hasta el 2020-04-09.
- Si el rango saliera de los datos, `dim_tiempo` cambiaría de tamaño con
  cada D.

`fct_pedidos` además tiene varias fechas que apuntan a esta dimensión
(role-playing, ADR 0033).

## Decisión

- **Rango fijo:** del 2016-01-01 al 2020-12-31 (1,827 días), con los
  extremos como `vars` en `dbt_project.yml` (`calendario_inicio`,
  `calendario_fin`). Se genera con `generate_series` de DuckDB (ADR 0036).
- **Clave:** `fecha` (tipo `DATE`), como en el contrato de la Fase 1; los
  hechos guardan fechas `DATE` como FK.
- **Atributos:** `fecha`, `anio`, `trimestre`, `mes`, `nombre_mes`, `dia`,
  `dia_semana` (ISO: 1 = lunes), `nombre_dia`, `semana_iso`, `anio_mes`
  (`AAAA-MM`), `es_fin_de_semana`, `es_feriado`, `nombre_feriado`.
- **Nombres en español con un mapeo explícito** (tabla de valores en SQL),
  no con funciones de formato que dependen del *locale* de la máquina.
- **Feriados:** los nacionales de Brasil 2016–2020 como seed de dbt
  (`fecha`, `nombre_feriado`), generado una vez con la librería `holidays`
  mediante un script versionado en el repo. El CSV se commitea; `holidays`
  es dependencia de desarrollo, no del pipeline.
- **Test `relationships`** de toda fecha de un hecho contra `dim_tiempo`:
  una fecha fuera del calendario detiene el pipeline.

## Alternativas consideradas

- **Rango derivado de los datos** (mínimo y máximo de Silver(D)): la
  dimensión cambiaría con D y una fecha futura nueva podría quedar afuera.
- **Calendario muy amplio** (por ejemplo 1900–2100): cubre todo sin pensar,
  pero son 73 mil filas para un dataset de dos años y esconde una fecha
  absurda en lugar de detectarla.
- **`dbt_utils.date_spine`:** funciona en cualquier motor, pero es verboso;
  `generate_series` de DuckDB lo dice en una línea (ADR 0036).
- **Clave entera `AAAAMMDD`:** la convención clásica de Kimball, pensada
  para motores donde comparar enteros era más barato. En DuckDB, un `DATE`
  es igual de eficiente y más legible.
- **Feriados calculados en SQL:** las fechas móviles (Carnaval, Viernes
  Santo, Corpus Christi) dependen de la Pascua; reimplementarlo en SQL es
  código frágil para una tabla de unas 60 filas.
- **Rango en `config/pipeline.yaml`:** no es un parámetro que cambie según
  el entorno, sino una propiedad del modelo; vive con el proyecto dbt.

## Por qué

Un calendario fijo es determinista: no depende de D ni de los datos. Y el
test de `relationships` convierte una fecha fuera de rango en una falla
explícita en vez de un join que pierde filas en silencio. El mapeo
explícito mantiene la reproducibilidad (el mismo resultado en Windows, en
la CI y en la imagen de Airflow).

## Consecuencias

- Si el dataset tuviera fechas posteriores a 2020, basta con cambiar las
  `vars`, regenerar el seed de feriados y hacer un full refresh.
- El seed de feriados tiene un test: toda fecha del seed existe en el
  calendario.
- Solo feriados nacionales; los estatales y municipales quedan fuera.
