# 0003 — Alcance del fail-fast: violaciones estructurales vs. normalización de catálogo

**Estado:** Aceptada
**Fecha:** 2026-08-19

## Contexto

El ADR 0001 fijó la semántica del fail-fast (hard-stop total del batch ante
cualquier fallo de validación), pero no definió qué cuenta como "fallo de
validación". La exploración de Fase 0 encontró casos reales en Olist que no
son errores de calidad sino huecos normales de un catálogo real:

- 610 de 32,951 productos (1.9%) sin `product_category_name`.
- 2 categorías de producto presentes en `products` sin fila correspondiente
  en `category_translation`.

Aplicar el ADR 0001 de forma literal a estos casos tumbaría el pipeline en
la primera corrida de Silver, por un problema de catálogo incompleto — no
por un dato corrupto o una violación de integridad real.

## Decisión

El fail-fast (hard-stop total, ADR 0001) se dispara solo ante
**violaciones estructurales**:

- Columna requerida nula donde el dominio de negocio no lo permite (ej.
  `order_id`, `customer_id`, `product_id`, `order_status`).
- Violación de PK tras aplicar la lógica de deduplicación documentada (ej.
  `review_id` duplicado en `order_reviews` después del paso de dedup).
- Registro huérfano en una FK que, según la exploración de Fase 0, no
  debería existir (ej. `order_items.product_id` sin fila en `products` —
  hoy son 0 huérfanos; si aparece uno, es una regresión real).
- Tipo de dato inválido / no parseable (ej. un precio que no convierte a
  numérico).
- Valor de medida imposible (ej. `price < 0`, `freight_value < 0`).

Los huecos de **catálogo/referencia** se normalizan con un valor por
defecto documentado, no disparan fail-fast:

- `product_category_name` nulo → `"sem_categoria"`.
- Categoría sin fila en `category_translation` →
  `product_category_name_english` cae al valor original en portugués.
- Timestamps de `orders` nulos cuando el `order_status` explica el nulo
  (ej. `order_delivered_customer_date` nulo en un pedido no entregado) —
  es un estado válido, no un dato faltante.

## Alternativas consideradas

- **Fail-fast estricto sin excepciones**: cualquier nulo o huérfano, sin
  distinción, tumba el batch. Más fiel a la letra original del ADR 0001,
  pero en la práctica hace inviable procesar el dataset real de Olist sin
  antes limpiar el catálogo a mano — no aporta al objetivo pedagógico de
  demostrar fail-fast sobre problemas reales de integridad.

## Por qué

Distinguir "dato roto" de "catálogo incompleto" es una decisión de diseño
real que cualquier pipeline de producción tiene que tomar — no es debilitar
el fail-fast, es definir su alcance con criterio. El ADR 0001 sigue vigente
tal cual para las violaciones estructurales listadas arriba.

## Consecuencias

- La capa Silver necesita un paso explícito de normalización de catálogo
  (categoría) antes/junto con la validación Pandera — documentado en
  `docs/schemas.md`.
- El listado de reglas "qué SÍ dispara fail-fast" queda fijado y debe
  usarse tal cual al implementar los schemas Pandera en la Fase 3 (no
  hay lugar para ambigüedad al momento de escribir código).
