# 0037 — Entrega tardía y retraso medidos en días calendario

**Estado:** Aceptada — supera parcialmente a la
[0033](0033-fct-pedidos-accumulating-snapshot.md) (cómo se calculan
`dias_retraso` y `es_entrega_tardia`)
**Fecha:** 2026-10-02

## Contexto

La ADR 0033 definió `dias_retraso` como "entrega real menos estimada", con
fracción, y `es_entrega_tardia` como entrega real posterior a la estimada,
comparando los timestamps completos.

Pero las dos fechas no tienen la misma precisión. La entrega real trae hora;
la estimada es un día: las 96,476 estimadas de Silver vienen a las
`00:00:00`. Comparar los timestamps cuenta como tardío a todo pedido
entregado *el mismo día prometido*, porque cualquier hora de ese día es
posterior a las 00:00. Medido sobre Silver al último día, por pedido
entregado:

| Criterio | Entregas tardías | % |
| --- | --- | --- |
| Timestamps completos (ADR 0033) | 7,827 | 8.1% |
| Días calendario | 6,535 | 6.8% |
| Diferencia: entregados el día prometido | 1,292 | 1.3 pts |

Además, con fracción, un pedido entregado a las 18:00 del día prometido
tendría `dias_retraso = 0.75` y, por día, no sería tardío: dos columnas de
la misma fila se contradirían.

## Decisión

- **`dias_retraso`** = días calendario entre la fecha estimada y la fecha de
  entrega (`date_diff('day', estimada, entrega)` sobre las fechas, no los
  timestamps). Entero (`BIGINT`): 0 si llegó el día prometido, negativo si
  llegó antes. Nulo mientras no hay entrega.
- **`es_entrega_tardia`** = la fecha de entrega es posterior a la fecha
  estimada, es decir `dias_retraso > 0`.
- `dias_hasta_aprobacion` y `dias_hasta_entrega` no cambian: miden
  duraciones entre dos instantes con hora, y ahí la fracción es real.

## Alternativas consideradas

- **Timestamps completos (ADR 0033):** técnicamente consistente, pero
  "tarde" pasa a significar "después del comienzo del día prometido", que
  no es lo que entiende nadie que lea el KPI.
- **Por día solo en `es_entrega_tardia`, con `dias_retraso` con fracción:**
  cambia menos, pero deja las dos columnas en contradicción en el día
  prometido.
- **Asumir una hora de corte para la promesa** (p. ej. fin del día, o una
  hora comercial): sería inventar un dato. Comparar con hora solo tendría
  sentido si la promesa la trajera, y en Olist no la trae.

## Por qué

Gold es la capa de consumo y se analiza en términos de negocio, no de los
datos crudos. La promesa al cliente es un día, así que se cumple o se
incumple por día. Una comparación más precisa que la promesa produce un KPI
engañoso por defecto, el mismo problema que llevó a `fct_reviews` a tener
grano en `review_id` (ADR 0034).

## Consecuencias

- El KPI de entregas tardías del criterio de "hecho" de la Fase 4 pasa de
  8.1% a 6.8%. El promedio de días hasta la entrega (12.56) no cambia.
- El contrato de `fct_pedidos` cambia el tipo de `dias_retraso` de `DOUBLE`
  a `BIGINT`.
- El fixture de Gold incluye una entrega a última hora del día prometido,
  que no debe contar como tardía.
