# 0030 — `dim_cliente` como SCD2 de persona, con `cliente_sk` como hash

**Estado:** Aceptada
**Fecha:** 2026-09-28

## Contexto

En Olist, `customer_id` es por pedido: cada compra genera uno nuevo. La
persona real es `customer_unique_id`. El contrato de la Fase 1 definía
`dim_cliente` con una fila por persona y su dirección, pero una persona
puede comprar desde direcciones distintas. Medido sobre el replay completo:
**252 personas tienen más de una dirección**; 39 cambian de estado, 83 de
ciudad y 130 solo de código postal, y 4 vuelven a una dirección anterior.

Con una fila por persona habría que elegir una dirección, y las ventas por
estado o ciudad quedarían mal atribuidas justo en esos casos.

Además, `silver_customers` es una tabla de referencia sin enmascarar (ADR
0018): a cualquier fecha D contiene los 99,441 `customer_id`, incluidos los
de compras futuras.

## Decisión

- **Grano:** una fila por **versión de dirección de una persona**
  (`customer_unique_id`). La dirección es `(customer_zip_code_prefix,
  customer_city, customer_state)`.
- **Construcción** (en `int_cliente_direcciones`): a partir de los pedidos
  **visibles en Silver(D)** unidos con `silver_customers` (no de
  `silver_customers` sola, que conoce el futuro), ordenados por
  `order_purchase_timestamp`. Se abre una versión nueva cada vez que la
  dirección difiere de la del pedido anterior de la persona. Volver a una
  dirección vieja es una versión nueva.
- **Vigencia:** `valid_from` = compra del primer pedido de la versión;
  `valid_to` = `valid_from` de la versión siguiente, nulo en la vigente;
  `is_current`. Intervalos semiabiertos `[valid_from, valid_to)`, igual
  que el SCD2 de Silver (ADR 0019).
- **Clave sustituta:** `cliente_sk =
  dbt_utils.generate_surrogate_key(['customer_unique_id', 'valid_from'])`,
  un hash determinista de la clave natural de la versión.
- **Cómo la resuelven los hechos:** `fct_pedidos` y `fct_pagos` la toman
  directo por `customer_id`, porque cada `customer_id` pertenece a
  exactamente una versión (la de su compra). Las reviews la resuelven por
  rango de fechas (ADR 0034).
- **Sin dbt snapshots.**

## Alternativas consideradas

- **Una fila por persona con la última dirección (SCD1):** simple, pero
  reescribe la historia: una venta despachada a São Paulo pasa a contarse
  en Río si la persona se mudó después.
- **Una fila por `customer_id`:** 99,441 filas, pero no representa a una
  persona, que es el sentido de esta dimensión (Fase 0).
- **dbt snapshots:** capturan cambios *comparando corridas*: la historia
  dependería de qué días se corrieron, y un full refresh la perdería. Aquí
  la historia ya está en los datos (cada pedido trae su dirección), así que
  se deriva completa en cada corrida. Los snapshots romperían la
  idempotencia y la equivalencia de la ADR 0029.
- **Clave entera secuencial** (`row_number()` por persona y
  `valid_from`): compacta y legible, pero **no es estable**. Una persona o
  una versión nueva desplaza la numeración de todas las filas siguientes, y
  los hechos incrementales guardan la clave del día en que se cargaron.
- **Clave entera con tabla de mapeo persistente:** estable, pero agrega una
  tabla con estado propio en una dimensión que se reconstruye completa.

## Por qué

Cada venta queda atribuida a la dirección desde la que se hizo, y la
dimensión se sigue pudiendo reconstruir desde Silver en cualquier D. La
clave es consecuencia directa de combinar dimensiones que se reconstruyen
(ADR 0029) con hechos incrementales: **la clave sustituta tiene que ser
función de la clave natural, no del orden de las filas**.
`(customer_unique_id, valid_from)` identifica cada versión, porque dos
versiones de la misma persona no pueden empezar en el mismo instante.

## Consecuencias

- La clave es un string MD5 en lugar de un entero; a este volumen es
  irrelevante.
- Tests (ADR 0036): `unique` y `not_null` en `cliente_sk`;
  `unique_combination_of_columns` en `(customer_unique_id, valid_from)`;
  `mutually_exclusive_ranges` por persona; una sola versión vigente por
  persona; cada hecho apunta a la versión vigente a su fecha. Si dos
  pedidos de la misma persona tuvieran la misma hora de compra y
  direcciones distintas, el test de unicidad lo detecta (medido: no pasa
  en Olist).
- Unit tests de `int_cliente_direcciones`: cambio solo de código postal,
  cambio de estado, vuelta a una dirección anterior, varias compras desde
  la misma dirección.
- Las coordenadas de la dirección son atributos de cada versión (ADR 0035).
- El resto de las dimensiones (producto, vendedor, estado, tiempo) no
  versionan y usan su clave natural.
