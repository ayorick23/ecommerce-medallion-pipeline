# 0025 — Geolocalización: descartar coordenadas fuera de Brasil antes de promediar

**Estado:** Aceptada
**Fecha:** 2026-09-25

## Contexto

`silver_geolocation_agg` promedia `lat`/`lng` por código postal
(`docs/schemas.md`, sección 3), porque la fuente trae 1,000,163 filas para
19,015 códigos. Medido sobre Olist, 42 filas en 21 códigos postales tienen
coordenadas fuera de Brasil. Con el promedio tal cual:

- el punto de 12 códigos cae fuera del país;
- 5 códigos tienen **todas** sus filas fuera (3 de ellos tienen una sola
  fila).

## Decisión

- Antes de promediar se descartan las filas con coordenadas fuera del
  rectángulo que forman los puntos extremos de Brasil: latitud entre
  -33.75 (Arroio Chuí) y 5.27 (Monte Caburaí), longitud entre -73.99
  (Serra do Divisor) y -34.79 (Ponta do Seixas).
- Los 5 códigos sin ninguna fila válida quedan fuera de la tabla:
  `silver_geolocation_agg` tiene 19,010 filas.
- Es una normalización documentada (ADR 0003), no un fail-fast: es ruido
  del catálogo, no un dato estructuralmente roto. Una coordenada que no se
  puede convertir a número sí sigue siendo fail-fast ("tipo no parseable").
- El rectángulo es una **constante en el código**, no un parámetro de
  `config/pipeline.yaml`. Los extremos de un país son un hecho geográfico,
  no algo que se ajuste por entorno; los 120 días de gracia de la ADR 0020
  sí son un parámetro.

## Alternativas consideradas

- **Promediar todo** (contrato de la Fase 1): 12 puntos mal ubicados en
  cualquier mapa de Gold.
- **Filtrar pero conservar los 5 códigos con coordenadas nulas:** agrega
  nulos sin aportar nada; un código sin coordenadas válidas equivale a uno
  sin geolocalización.
- **Mediana en lugar de promedio:** robusta cuando un código tiene muchas
  filas, pero no corrige los códigos de una sola fila.

## Por qué

Un código sin geolocalización válida es el mismo caso que un código sin
geolocalización, que ya existe y ya es válido (278 clientes y 7 vendedores,
porque la tabla es de consulta, no una FK). No se introduce ningún caso
nuevo para los consumidores.

## Consecuencias

- El rectángulo es aproximado: incluye franjas de países vecinos. Alcanza
  para descartar los errores gruesos de la fuente, pero no garantiza que un
  punto esté dentro de Brasil.
- El criterio de "hecho" de la Fase 3 concilia `silver_geolocation_agg`
  contra 19,010 filas, no contra los 19,015 códigos de la fuente.
