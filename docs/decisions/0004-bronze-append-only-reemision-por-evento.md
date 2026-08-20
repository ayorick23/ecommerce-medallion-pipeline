# 0004 — Bronze append-only con re-emisión por evento para simular llegada incremental

**Estado:** Aceptada
**Fecha:** 2026-08-19

## Contexto

Olist no provee un log de cambios: `orders` trae una sola fila por pedido
con el estado final conocido, no un historial de transiciones. El plan
exige simular llegada diaria incremental, incluyendo el caso de un pedido
que "cambia de estado en un día distinto al de su ingesta inicial". Hay que
decidir cómo Bronze representa eso sin dejar de ser una copia cruda de la
fuente (principio rector #1 del plan: diseño antes que código; ADR 0002:
la lógica de negocio no vive en la orquestación, y por extensión tampoco
debería inventarse en Bronze).

## Decisión

`orders` trae 4 timestamps que sí representan eventos reales:
`order_purchase_timestamp`, `order_approved_at`,
`order_delivered_carrier_date`, `order_delivered_customer_date`. Bronze
**re-emite la fila cruda completa y sin modificar** de un pedido en cada
día simulado que corresponda a uno de sus timestamps no nulos. Es decir:
un mismo `order_id` puede aparecer en Bronze varias veces, en particiones
de días distintos — siempre con el mismo contenido crudo (la fila no
cambia, solo se repite en el día relevante). Bronze nunca hace upsert ni
interpreta cuál timestamp "significa" qué estado — eso es trabajo de
Silver.

`order_reviews` sigue la misma lógica pero con un solo evento propio:
`review_creation_date`.

`order_items` y `order_payments` no tienen timestamp de evento propio
utilizable (`shipping_limit_date` es una fecha límite futura, no un
evento pasado; `order_payments` no trae fecha) — se ingieren junto con el
día de `order_purchase_timestamp` de su pedido padre, una sola vez.

Las tablas de referencia sin dimensión temporal en la fuente (`customers`,
`products`, `sellers`, `geolocation`, `category_translation`) se cargan
como snapshot completo, no particionado por día — recargar el mismo
snapshot es naturalmente idempotente porque el contenido no cambia.

La interpretación de esos 4 timestamps como los eventos etiquetados
`created / approved / shipped / delivered`, y la construcción del
historial SCD2 a partir de las re-emisiones de Bronze, ocurre en **Silver**
(ver `docs/schemas.md`), no en Bronze.

## Alternativas consideradas

- **Derivar los eventos etiquetados directamente en Bronze**: se descartó
  porque mapear un timestamp a una etiqueta de negocio (`"shipped"`) es
  lógica de negocio, y el ADR 0002 ya estableció que esa lógica vive fuera
  de la capa de ingesta cruda. Mezclarla en Bronze rompe la garantía de
  que Bronze siempre se puede reconstruir mirando solo la fuente original.
- **Cargar `orders` completo cada día (snapshot diario)**: más simple de
  escribir, pero desperdicia espacio (miles de filas repetidas sin razón
  la mayoría de los días) y no deja claro qué disparó la aparición de un
  pedido en un día dado — se pierde la trazabilidad del evento.

## Por qué

Re-emitir la fila cruda tal cual, en cada día que le corresponde por
timestamp, mantiene a Bronze fiel a "copia sin transformar de la fuente"
y a la vez resuelve el replay incremental sin inventar datos que Olist no
tiene. La idempotencia queda simple de razonar: reprocesar el día D vuelve
a escribir exactamente las mismas filas para los mismos `order_id`.

## Consecuencias

- La clave de partición/dedup de `bronze_orders` es
  `(order_id, dia_simulado)`, no `order_id` solo.
- Silver es responsable de colapsar las re-emisiones de un mismo
  `order_id` en un historial SCD2 de 4 eventos como máximo (ver
  `docs/schemas.md`, sección SCD2).
- Bronze de `order_items`/`order_payments` no vuelve a re-emitirse en
  días posteriores del pedido — su contenido no cambia con el estado.
