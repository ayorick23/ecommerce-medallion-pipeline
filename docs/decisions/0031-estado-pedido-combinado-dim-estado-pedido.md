# 0031 — Estado del pedido combinado y `dim_estado_pedido` con 6 estados

**Estado:** Aceptada
**Fecha:** 2026-09-28

## Contexto

El estado de un pedido en Silver sale de dos lugares que no coinciden:

- El **SCD2** (`silver_order_status_history`, ADR 0019): la progresión
  `creado → aprobado → despachado → entregado`, con timestamps y
  enmascarada a la fecha D (ADR 0018).
- **`order_status`**: el estado final según la fuente. Es el único dato
  futuro que Silver no puede enmascarar, porque `canceled`, `unavailable`
  y otros estados no tienen timestamp en Olist (ADR 0018).

El SCD2 no puede representar las cancelaciones: un pedido cancelado
después de aprobarse tiene `aprobado` como vigente. Medido en la fuente:
625 pedidos `canceled` (6 de ellos con fecha de entrega), 609
`unavailable`, 314 `invoiced` y 301 `processing`. `docs/schemas.md`
(sección 5) dejó este punto pendiente para la Fase 4.

## Decisión

- **Estado combinado** (en `int_pedido_estado`), por pedido y a la fecha
  D:
  1. `order_status = canceled` → `cancelado`.
  2. `order_status = unavailable` → `no_disponible`.
  3. En cualquier otro caso, el `status_event` de la fila vigente del SCD2.

  La cancelación gana siempre, incluso en los 6 pedidos cancelados después
  de entregados: la fuente dice que el estado final es ese.
- `invoiced` y `processing` no son estados propios: se absorben en la
  progresión del SCD2 (normalmente `aprobado`). No tienen timestamp, así
  que no se sabe cuándo empiezan.
- **`dim_estado_pedido`**, como seed de dbt, con 6 filas:

  | `estado` | `categoria` | `es_terminal` | `orden` |
  | --- | --- | --- | --- |
  | `creado` | `en_curso` | no | 1 |
  | `aprobado` | `en_curso` | no | 2 |
  | `despachado` | `en_curso` | no | 3 |
  | `entregado` | `completado` | sí | 4 |
  | `cancelado` | `cancelado` | sí | 5 |
  | `no_disponible` | `cancelado` | sí | 6 |

  Más una `descripcion` en texto. `fct_pedidos` tiene `estado` como FK.

## Alternativas consideradas

- **Solo el SCD2:** respeta el tiempo, pero 1,234 pedidos cancelados o no
  disponibles quedarían "en curso" para siempre, y las tasas de
  cancelación darían 0.
- **Solo `order_status`:** muestra el estado *final* en cualquier D. Un
  pedido comprado hoy y entregado en dos semanas figuraría `delivered`
  desde el primer día: la fuga del futuro que la ADR 0018 evita, aplicada a
  casi todos los pedidos en lugar de a 1,234.
- **Los 8 valores crudos de la fuente como estados:** `invoiced`,
  `processing`, `created` y `approved` no tienen timestamp, así que usarlos
  filtraría el futuro igual que la opción anterior, y mezclaría el
  vocabulario de la fuente con el de la progresión.

## Por qué

Se toma de cada fuente lo que sabe hacer bien: el tiempo del SCD2 y el
desenlace de `order_status`. El costo queda acotado y documentado: la fuga
de la ADR 0018 afecta solo a los 1,234 pedidos cancelados o no
disponibles, que figuran así desde su compra. Las columnas `categoria`,
`es_terminal` y `orden` permiten las preguntas típicas (tasa de
cancelación, pedidos abiertos, embudo de etapas) sin repetir `CASE` en cada
consulta.

## Consecuencias

- Un pedido cancelado figura `cancelado` desde el día de su compra (fuga
  aceptada, ADR 0018). Se documenta en `docs/schemas.md`.
- `fct_pedidos` guarda solo el estado **actual** a la fecha D. La pregunta
  "¿en qué estado estaba en el instante X?" la sigue respondiendo el SCD2
  de Silver (sección 5 de `docs/schemas.md`).
- Tests: `accepted_values` y `relationships` de `estado`. Unit tests de
  `int_pedido_estado`: progresión normal, cancelado antes de aprobarse,
  cancelado después de entregado, `unavailable`, `invoiced` y
  `processing`.
