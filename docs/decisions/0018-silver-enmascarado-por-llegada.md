# 0018 — Silver enmascara los eventos que todavía no ocurrieron a la fecha D

**Estado:** Aceptada
**Fecha:** 2026-09-25

## Contexto

Por la ADR 0004, Bronze re-emite la **fila cruda completa** de un pedido en
cada día de evento. Esa fila trae desde el primer día los 4 timestamps y el
`order_status` **final** del pedido: un pedido comprado el 2018-06-11 ya
dice `delivered` y trae su fecha de entrega (2018-06-21) en la partición
del 06-11. Entre la compra y el último evento de un pedido pasan 10 días de
mediana, 29 en el percentil 95 y hasta 210. Lo mismo pasa con
`review_answer_timestamp`, que llega en la fila de la review el día de su
creación aunque la respuesta sea posterior (1 día de mediana, hasta 518).

Si Silver(D) usara la fila tal cual, "conocería el futuro": el historial de
estados al día D incluiría eventos que aún no ocurrieron, y las
re-emisiones de Bronze serían decorativas (solo importaría la primera).

## Decisión

Un evento existe para Silver(D) solo si ocurrió **hasta el final del día
D**. La granularidad es el día: la corrida de D representa el cierre de ese
día.

- **Pedidos:** un pedido existe en Silver(D) si su día de compra es `<= D`.
  En `silver_orders`, `order_approved_at`, `order_delivered_carrier_date` y
  `order_delivered_customer_date` quedan nulos si su evento todavía no
  ocurrió. Qué eventos ocurrieron lo decide el `valid_from` ajustado del
  SCD2 (ADR 0019), para que `silver_orders` y el historial nunca se
  contradigan; el valor que se muestra es el crudo de la fuente.
- **Items y pagos:** existen si su pedido existe (llegan con la compra, ADR
  0004).
- **Reviews:** una review existe si su `review_creation_date` es `<= D` (y su
  pedido existe, ADR 0020). `review_answer_timestamp` queda nulo si su día
  es posterior a D.
- **No se enmascaran** los datos que se conocen desde la compra aunque
  apunten al futuro: `order_estimated_delivery_date` (promesa de entrega) y
  `shipping_limit_date` (plazo del vendedor).
- **`order_status` se conserva** como "estado final según la fuente": es el
  único dato futuro que no se puede enmascarar, porque `canceled`,
  `unavailable` y otros estados no tienen timestamp en Olist. El estado
  vigente a la fecha D es el de la fila `is_current` del SCD2.
- Las tablas de referencia no se enmascaran: no tienen dimensión temporal
  (limitación ya documentada en `docs/schemas.md`, "los snapshots conocen
  el futuro").

## Alternativas consideradas

- **Confiar en la fila cruda:** lo más simple, pero Silver conoce el futuro
  y la simulación incremental de la Fase 2 pierde sentido para los pedidos.
- **Enmascarar solo el SCD2** y dejar `silver_orders` crudo: las dos tablas
  se contradicen (una dice "despachado", la otra ya trae fecha de entrega).

## Por qué

Silver(D) queda igual a lo que un sistema real sabría el día D: sin fuga de
información futura (*data leakage*), con un historial de estados que
evoluciona día a día y con las re-emisiones de Bronze cumpliendo su
función. Con la reconstrucción completa (ADR 0017), el enmascarado es un
filtro por fecha, no una lógica de merge.

## Consecuencias

- `order_status` y el estado vigente del SCD2 pueden diferir a propósito
  (p. ej. un pedido cancelado después de aprobarse: SCD2 vigente
  `aprobado`, `order_status = canceled`). Cómo combinarlos en
  `dim_estado_pedido` queda para la Fase 4.
- El criterio de "hecho" de la Fase 3 incluye probar que Silver(D) no
  contiene ningún evento posterior a D.
