# 0033 — `fct_pedidos` como accumulating snapshot con fechas role-playing

**Estado:** Aceptada — parcialmente superada por
[0037](0037-entrega-tardia-por-dia-calendario.md) (`dias_retraso` y
`es_entrega_tardia` se miden en días calendario)
**Fecha:** 2026-09-28

## Contexto

El contrato de la Fase 1 definía `fct_pedidos` con grano de línea de pedido
`(order_id, order_item_id)`, una sola fecha (la de compra) y el estado
actual. Pero las preguntas más interesantes de Olist son de **proceso**:
cuánto tarda un pedido en aprobarse o en entregarse, y cuántos llegan
tarde. Un pedido pasa por etapas con fechas propias, que se van llenando a
medida que ocurren.

Kimball distingue tres tipos de tabla de hechos: de transacción (una fila
por evento), snapshot periódico (una fila por entidad y período) y
**accumulating snapshot** (una fila por instancia de un proceso con etapas
bien definidas, que se actualiza cuando pasa cada etapa).

## Decisión

- `fct_pedidos` es un **accumulating snapshot**. Mantiene el grano de línea
  `(order_id, order_item_id)`, y cada fila se actualiza (merge, ADR 0029)
  a medida que su pedido avanza.
- **FKs:**
  - `cliente_sk` (ADR 0030), `product_id`, `seller_id` y `estado` (ADR
    0031).
  - A `dim_tiempo`, con un rol cada una: `fecha_compra`,
    `fecha_aprobacion`, `fecha_despacho`, `fecha_entrega`,
    `fecha_entrega_estimada` y `fecha_limite_envio` (del
    `shipping_limit_date` de la línea).
  - Las fechas de etapas que todavía no ocurrieron a la fecha D quedan
    nulas.
- **Medidas:**
  - Por línea: `price`, `freight_value` (`DECIMAL(18,2)`, ADR 0022).
  - De proceso, calculadas desde los timestamps en días con fracción:
    `dias_hasta_aprobacion`, `dias_hasta_entrega` (de la compra a la
    entrega al cliente) y `dias_retraso` (entrega real menos estimada;
    negativo si llegó antes). También `es_entrega_tardia`. Todas nulas
    mientras la etapa no ocurrió.
- Se conserva el grano de línea: **no** hay un hecho aparte a nivel de
  pedido.

## Alternativas consideradas

- **Hecho de transacción de eventos de estado:** ya existe en Silver
  (`silver_order_status_history`), y la sección 5 de `docs/schemas.md`
  decidió no duplicarlo en Gold.
- **Snapshot periódico** (una fila por línea y día): responde "¿cuántos
  pedidos abiertos había cada día?", pero multiplica las filas por cientos
  de días, y esa pregunta la responde el SCD2.
- **Un segundo hecho a nivel de pedido (`fct_ordenes`)** para las medidas
  de proceso: evita la repetición por línea, pero duplica hechos y claves
  para un problema que se resuelve contando pedidos distintos.
- **Solo la fecha de compra**, como en el contrato de la Fase 1: las
  preguntas de tiempos de proceso quedarían fuera de Gold.

## Por qué

El accumulating snapshot es el tipo de hecho diseñado exactamente para
procesos con etapas, y es un patrón de Kimball que se reconoce en una
entrevista. Las fechas role-playing contra una sola `dim_tiempo` evitan
tener seis calendarios.

## Consecuencias

- **Las fechas y medidas de proceso son del pedido, pero se repiten en
  cada línea.** Los KPIs por pedido se calculan con conteo o agregación
  sobre pedidos distintos (`COUNT(DISTINCT order_id)`, o agregando primero
  por pedido). Promediar `dias_hasta_entrega` fila por fila da más peso a
  los pedidos con más líneas. Se documenta en `docs/schemas.md` y en la
  descripción del modelo. `price` y `freight_value` sí son aditivas por
  línea.
- Medido sobre Silver al último día, por pedido entregado: 12.56 días
  promedio de la compra a la entrega (con fracción; 12.50 si se cuentan
  días calendario) y 8.1% de entregas tardías. Promediando fila por fila
  da 12.47: el sesgo de la advertencia anterior, con números reales.
- Toda fecha role-playing tiene su test de `relationships` contra
  `dim_tiempo` (ADR 0032).
