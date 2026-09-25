# 0022 — Montos como `Decimal(18,2)`

**Estado:** Aceptada
**Fecha:** 2026-09-25

## Contexto

Bronze guarda todo como texto (ADR 0011). Silver tiene que elegir un tipo
para los montos `price`, `freight_value` y `payment_value`. En la fuente,
todos tienen como máximo 2 decimales. Medido con el stack del proyecto
(Polars 1.43, Pandera 0.32, DuckDB 1.5):

- Suma de todos los pagos: `16008872.120000012` con `Float64`,
  `16008872.12` con `Decimal`.
- Polars, Pandera y DuckDB soportan `Decimal`, y el tipo pasa intacto por
  Parquet (DuckDB lo lee como `DECIMAL(18,2)`).
- Al convertir el texto `"12.345"` a `Decimal(18,2)`, Polars lo **trunca
  en silencio** a `12.34`, sin error.

## Decisión

- Los montos se guardan en Silver como `Decimal(18,2)`. Polars y DuckDB
  amplían solos la precisión a `(38,2)` al sumar.
- Nueva regla fail-fast, dentro de "tipo no parseable": un monto debe
  cumplir `^-?\d+(\.\d{1,2})?$` antes de convertirse. Un valor con más de
  2 decimales, o en otro formato, detiene el pipeline en lugar de
  truncarse. El signo se admite en el formato para que un negativo se
  reporte como "medida imposible" (regla de rango), no como formato
  inválido.
- Las coordenadas (`lat`/`lng`) siguen en `Float64`: no son montos y se
  promedian.

## Alternativas consideradas

- **`Float64`:** lo más común con Olist, pero introduce errores de
  redondeo binario en las sumas y obliga a comparar montos con tolerancia.
  Es el tipo equivocado para dinero.
- **Enteros en centavos (`Int64`):** exactos y rápidos, pero cada
  consumidor tiene que dividir por 100, una fuente clásica de errores en
  Gold y BI.

## Por qué

`Decimal` es el tipo correcto para dinero, exacto y soportado por todo el
stack sin costo. 18 dígitos entran en un entero de 64 bits (eficiente en
DuckDB) y sobra margen: el pago máximo ronda los 13 mil y el total, los 16
millones. La regla de formato cubre el único riesgo que introduce el tipo,
que un dato cambie de valor en silencio al convertirse.

## Consecuencias

- La suma de items y la de pagos de un pedido se pueden comparar por
  igualdad exacta (p. ej. el pedido `03ecec24…`: 375.73 y 375.73).
- dbt/DuckDB (Fase 4) leen `DECIMAL(18,2)` directamente desde Parquet.
