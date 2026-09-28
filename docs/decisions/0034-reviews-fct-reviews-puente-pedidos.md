# 0034 — Reviews: `fct_reviews` más una tabla puente con los pedidos

**Estado:** Aceptada
**Fecha:** 2026-09-28

## Contexto

El plan original de Gold no incluía las reviews, pero son la única medida
de satisfacción de Olist. La relación review–pedido es de muchos a muchos
(ADR 0021): hay 98,410 reviews distintas y 99,224 vínculos
`(review_id, order_id)`, porque una misma review puede estar asociada a
varios pedidos de la misma persona. En Silver, un `COUNT(*)` cuenta 814
reviews de más, y un promedio de `review_score` por fila da 4.0864 en lugar
de 4.0888 (sección 3 de `docs/schemas.md`).

## Decisión

- **`fct_reviews`**, con una fila por `review_id` (98,410):
  - Medidas: `review_score`, `tiene_comentario` (hay un
    `review_comment_message` no vacío) y `dias_hasta_respuesta` (nula
    mientras no hay respuesta a la fecha D).
  - FKs: `fecha_creacion` y `fecha_respuesta` a `dim_tiempo`, y
    `cliente_sk`.
  - Los textos de los comentarios no pasan a Gold: se quedan en Silver.
- **`puente_review_pedido`**, con una fila por vínculo `(review_id,
  order_id)` (99,224), para llegar desde una review a sus pedidos (y desde
  ahí a productos y vendedores).
- **`cliente_sk` por rango de fechas:** la versión del cliente (ADR 0030)
  vigente a la `review_creation_date`. Si la review es anterior a la
  primera versión de la persona, se toma la primera. Esto pasa con las
  reviews adelantadas (ADR 0020), creadas hasta 111 días antes de su
  pedido.
- Ambas tablas son incrementales con merge (ADR 0029): `fct_reviews` por
  `review_id` y el puente por `(review_id, order_id)`.

## Alternativas consideradas

- **Una fila por vínculo, como en Silver:** replica el problema. Cada
  `COUNT` y cada `AVG` necesitaría un `DISTINCT` o una corrección, y quien
  no lo sepa obtiene un número incorrecto sin darse cuenta.
- **Asignar cada review a un pedido "principal":** evita el puente, pero la
  elección es arbitraria y se pierde el vínculo con los demás pedidos.
- **Medidas de la review dentro de `fct_pedidos`:** el grano no coincide
  (la review es del pedido y `fct_pedidos` es por línea), así que el score
  se repetiría por cada línea y por cada pedido de la review.
- **`cliente_sk` resuelto por el pedido**, como en `fct_pedidos`: con
  varios pedidos por review no hay un pedido único del cual tomarlo, y la
  review ocurre en su propia fecha.

## Por qué

Con el grano en `review_id`, **las agregaciones por defecto son
correctas**: `COUNT(*)` y `AVG(review_score)` sobre `fct_reviews` dan 98,410
y 4.0888 sin trucos. El muchos a muchos queda aislado en el puente, que es
el patrón estándar de Kimball (*bridge table*) para esa relación.

## Consecuencias

- Analizar reviews por producto o vendedor pasa por el puente y
  `fct_pedidos`, y ahí una review cuenta en cada línea a la que llega: se
  documenta que esos cruces requieren decidir cómo ponderar.
- Tests: `review_id` único en `fct_reviews`; `(review_id, order_id)` único
  en el puente; `relationships` del puente contra `fct_reviews` y contra
  los pedidos; `review_score` en `[1, 5]`; todos los pedidos de una review
  pertenecen a la misma persona (medido: se cumple en las 98,410).
- `tiene_comentario` mira solo el mensaje, no el título: las 1,721 reviews
  con título pero sin mensaje cuentan como sin comentario. Los 9 mensajes
  vacíos tampoco cuentan.
