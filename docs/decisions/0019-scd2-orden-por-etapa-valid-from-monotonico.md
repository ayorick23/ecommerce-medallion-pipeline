# 0019 — SCD2 de estados: orden por etapa y `valid_from` que nunca retrocede

**Estado:** Aceptada
**Fecha:** 2026-09-25

## Contexto

El diseño de `silver_order_status_history` (Fase 1, `docs/schemas.md`
sección 5) despliega los 4 timestamps de un pedido en eventos
`creado / aprobado / despachado / entregado` y los ordena
cronológicamente. Medido sobre Olist, los timestamps no siempre respetan
el orden lógico del pedido: **1,382 pedidos (1.4%)** tienen alguno fuera
de orden.

| Anomalía | Pedidos |
| --- | --- |
| Despachado antes de aprobarse | 1,359 (mediana 17 h, máximo 171 días) |
| Despachado antes de la compra (por hora) | 166, de los cuales 2 caen en un día anterior |
| Entregado al cliente antes de despacharse | 23 |

Además, 14 pedidos tienen despacho sin aprobación y 1 tiene entrega sin
despacho (etapas salteadas).

Ordenar por hora hace que el historial "retroceda" (un pedido despachado
vuelve a "aprobado") y, en los 23 casos de entrega antes del despacho,
deja como vigente "despachado" en un pedido ya entregado.

## Decisión

- Los eventos se ordenan por **etapa**: `creado` (1) < `aprobado` (2) <
  `despachado` (3) < `entregado` (4). Solo los timestamps no nulos generan
  fila; una etapa salteada no genera fila.
- `valid_from` de cada evento = el máximo entre su timestamp crudo y el
  `valid_from` del evento anterior del mismo pedido. Nunca retrocede.
- El timestamp crudo se conserva en `event_timestamp`, y `is_adjusted`
  marca las filas donde `valid_from` difiere de él.
- `valid_to` = `valid_from` del siguiente evento visible del mismo pedido;
  nulo en el último. `is_current` es verdadero solo en la fila con
  `valid_to` nulo.
- La visibilidad a la fecha D (ADR 0018) usa `valid_from`: un evento existe
  en Silver(D) si `valid_from` cae en un día `<= D`.
- Los intervalos son semiabiertos, `[valid_from, valid_to)`.

## Alternativas consideradas

- **Orden cronológico por timestamp crudo** (diseño de la Fase 1): el
  historial retrocede y el estado vigente puede quedar mal.
- **Orden por etapa con timestamps crudos:** el vigente es correcto, pero
  quedan intervalos negativos o superpuestos y la consulta "estado en el
  instante X" puede devolver 0 o 2 filas.
- **Fail-fast o excluir los pedidos anómalos:** son 1,382 casos que no son
  datos rotos; frenar el pipeline o perder el 1.4% de los pedidos no se
  justifica.

## Por qué

La regla básica de un SCD2 es que en cada instante haya una y solo una
versión vigente. Esta es la única alternativa que la cumple sin perder
información: el dato crudo sigue disponible en `event_timestamp`. Es una
normalización documentada de una anomalía de la fuente, en el sentido de la
ADR 0003, no una violación estructural.

## Consecuencias

- Ejemplo (pedido `000576fe…`): despacho crudo 07-05 12:15:00, aprobación
  07-05 16:35:48 → el despacho queda con `valid_from` 07-05 16:35:48 e
  `is_adjusted = true`.
- Pueden quedar intervalos de **duración cero** (`valid_from = valid_to`,
  como "aprobado" en el ejemplo): el pedido pasó por esa etapa, pero
  ninguna consulta "en el instante X" lo encuentra en ella. Es el
  comportamiento correcto para un dato de origen contradictorio.
- Los 2 pedidos despachados en un día anterior a su compra aparecen en
  Silver el día de la compra, junto con sus items y pagos: su despacho
  ajustado no puede ser anterior a su creación.
- Silver reconoce un evento ajustado el día de su `valid_from`, que puede
  ser posterior al día en que la fila llegó a Bronze. Es intencional.
