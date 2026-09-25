# 0021 — Ajustes al contrato de Silver a partir de datos medidos

**Estado:** Aceptada
**Fecha:** 2026-09-25
**Supera parcialmente a:** [0003](0003-alcance-fail-fast-normalizacion-vs-hard-stop.md)
(solo el ejemplo de deduplicación de reviews; el alcance del fail-fast
sigue vigente)

## Contexto

El contrato de Silver (`docs/schemas.md`, Fase 1) se escribió antes de
medir algunos supuestos sobre el dataset. Al diseñar la Fase 3 se midieron,
y cuatro no se sostienen.

## Decisión

1. **Reviews: PK `(review_id, order_id)`.** 789 `review_id` aparecen en
   más de una fila (1,603 filas: 764 con 2 copias, 25 con 3). En los 789
   casos el contenido es idéntico entre copias y los pedidos son de la
   misma persona (`customer_unique_id`): es una encuesta asociada a varios
   pedidos. En sentido inverso, 547 pedidos tienen 2 o 3 reviews. La
   relación es de muchos a muchos. `silver_order_reviews` conserva las
   99,224 filas, y la PK es el par, que no tiene duplicados. Se elimina la
   regla "deduplicar por `review_id` quedándose con la más reciente": las
   copias empatan en todo, así que la elección sería arbitraria, y borraría
   814 vínculos reales.
2. **`payment_installments >= 0`** (antes `>= 1`). 2 pagos tienen 0 cuotas,
   ambos `credit_card` con `payment_sequential = 2`, es decir, el segundo
   medio de pago de un pedido. Hipótesis, no verificable con los datos: un
   pago combinado (millas y tarjeta, o dos tarjetas). El valor se conserva
   tal cual; lo imposible sigue siendo una cantidad negativa.
3. **Códigos postales como texto.** Los códigos de clientes, vendedores y
   geolocalización tienen siempre 5 caracteres; 23,995 clientes (24%)
   tienen uno que empieza con 0. Silver los guarda como `String` y valida
   el formato `^\d{5}$`.
4. **Errata de la fuente corregida en Silver:** `product_name_lenght` y
   `product_description_lenght` pasan a `product_name_length` y
   `product_description_length`.

## Alternativas consideradas

- **Reviews en dos tablas** (una por review, 98,410 filas, más una tabla
  puente review–pedido): el modelo relacional "de libro", pero agrega una
  tabla y un join sin necesidad a este volumen.
- **Cuotas en 0 normalizadas a 1, o convertidas en nulo:** la primera
  inventa una interpretación; la segunda pierde el valor original. Listar
  esos 2 pagos como excepciones conocidas no escala.
- **Códigos postales como entero:** corrompe el 24% de los clientes.

## Por qué

Silver refleja la fuente tal como es, no como la supusimos en la Fase 1.
El fail-fast existe para detectar datos rotos, no para imponer lo que
esperábamos: cuando una regla choca con datos legítimos, el error está en
la regla.

## Consecuencias

- Para contar reviews hay que usar `COUNT(DISTINCT review_id)`: un
  `COUNT(*)` suma 814 de más. Queda documentado en el contrato y conviene
  cubrirlo en Gold (Fase 4).
- El diagrama de Gold en `docs/schemas.md` pasa los códigos postales a
  `string`.
- 3 pagos `not_defined` con monto 0 no violan ninguna regla
  (`payment_value >= 0`) y se conservan.
