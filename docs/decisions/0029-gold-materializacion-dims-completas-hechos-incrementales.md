# 0029 — Materialización de Gold: dimensiones completas, hechos incrementales

**Estado:** Aceptada
**Fecha:** 2026-09-28

## Contexto

Silver se reconstruye completo en cada corrida (ADR 0017). Para Gold hay
que elegir cómo se materializa cada modelo: reconstruir la tabla completa
en cada corrida, o cargar solo lo nuevo (incremental) y fusionarlo con lo
que ya había. Uno de los objetivos de aprendizaje de la fase es justamente
"tabla completa vs. incremental/merge" (plan, Fase 4).

Los hechos cambian con el tiempo: una línea de pedido se actualiza al
aprobarse, despacharse y entregarse (ADR 0033), una review recibe su
respuesta días después, y una review adelantada entra recién cuando llega
su pedido (ADR 0020).

## Decisión

- **Dimensiones:** tabla completa, reconstruida en cada corrida. Son
  chicas y sus claves son estables aunque se reconstruyan (ADR 0030).
- **Hechos:** incrementales con una **marca de agua** por fila,
  `_visible_desde`: el instante a partir del cual el contenido actual de la
  fila es visible en Silver. En cada corrida se procesan solo las filas con
  `_visible_desde` mayor al máximo ya cargado.

  | Hecho | Estrategia | Clave del merge | `_visible_desde` |
  | --- | --- | --- | --- |
  | `fct_pedidos` | merge | `(order_id, order_item_id)` | `valid_from` del último evento visible del pedido (SCD2, ADR 0019) |
  | `fct_pagos` | append | — | compra del pedido: los pagos llegan con ella y no cambian |
  | `fct_reviews` | merge | `review_id` | el mayor entre la creación de la review, la compra de su primer pedido visible y la respuesta, si ya la hay |
  | `puente_review_pedido` | merge | `(review_id, order_id)` | el mayor entre la creación de la review y la compra de ese pedido |

  La marca de agua de las reviews **no puede ser solo la fecha de
  creación**: una review adelantada tiene una creación anterior a la marca
  ya cargada, y entra a Silver recién cuando llega su pedido. Tomar el
  mayor de los instantes que la hacen visible cubre ese caso y el de la
  respuesta que llega después.
- **Reprocesar hacia atrás** (volver a un D anterior al de Gold) requiere
  `--full-refresh`; el comando lo exige (ADR 0028).
- **Equivalencia:** Gold incremental, construido día a día, debe ser
  idéntico a un full refresh en el mismo D. Es parte del criterio de
  "hecho" de la fase y se prueba (ADR 0036).

## Alternativas consideradas

- **Todo como tabla completa:** a este volumen (112,650 líneas) alcanza de
  sobra y es más simple. Se descarta solo porque la estrategia incremental
  es un objetivo de aprendizaje explícito de la fase y el patrón que se usa
  a escala.
- **Todo incremental, dimensiones incluidas:** una dimensión SCD2
  incremental necesita cerrar versiones anteriores con un merge propio, y
  a este tamaño no aporta nada.
- **Marca de agua por día de corrida** (procesar todo lo de Silver con
  fecha D): se rompe si se saltea un día o se corre dos veces, y no ve las
  reviews adelantadas. La marca derivada de los datos no depende del
  historial de corridas.
- **Append también en `fct_pedidos`:** una línea que se entrega días
  después tendría dos filas; el grano se rompería.

## Por qué

Es el patrón estándar para hechos que se actualizan (accumulating snapshot,
ADR 0033), y la marca de agua sale de los datos, no del reloj, así que el
resultado depende solo de Silver(D) y no de qué días se corrieron antes. La
honestidad obliga a decirlo: a este volumen, la tabla completa bastaba.
Esta decisión se toma por el aprendizaje y por el patrón, no por el
rendimiento.

## Consecuencias

- `_visible_desde` es una columna técnica de cada hecho (con prefijo `_`),
  no una medida.
- `fct_pagos` en append depende de que todos los pagos de un pedido lleguen
  juntos, con la compra (ADR 0018); la equivalencia y la idempotencia lo
  prueban.
- El test de equivalencia prueba además la estabilidad de `cliente_sk`
  (ADR 0030): una fila cargada hace días guarda la clave de entonces, y
  tiene que coincidir con la del full refresh.
- Si en el futuro aparece un cambio en Silver que no avanza ninguna marca
  de agua, el incremental no lo ve. La equivalencia es la red que lo
  detecta, y `--full-refresh` es el remedio.
