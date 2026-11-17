# 0039 — Entrega tardía y retraso medidos en días calendario

**Estado:** Aceptada — supera parcialmente a la
[0033](0033-fct-pedidos-accumulating-snapshot.md) (cómo se calculan
`dias_retraso` y `es_entrega_tardia`)
**Fecha:** 2026-11-15

> Esta decisión se tomó el 2026-10-02 en la rama `feat/fase-4-gold`, con
> el número 0037, pero esa rama nunca se mergeó: el PR ya estaba cerrado.
> Se rescata con número nuevo porque la 0037 de `main` es otra decisión
> (ADR 0038, "Estado perdido entre máquinas").

## Contexto

La ADR 0033 definió `dias_retraso` como "entrega real menos estimada", con
fracción, y `es_entrega_tardia` como entrega real posterior a la estimada,
comparando timestamps completos.

Las dos fechas no tienen la misma precisión. La entrega real trae hora; la
estimada es un día: las 99,441 fechas estimadas de Silver vienen a las
`00:00:00`, sin excepción. Comparar timestamps cuenta como tardío a todo
pedido entregado *el mismo día prometido*, porque cualquier hora de ese día
es posterior a las 00:00. Medido sobre Gold al 2018-10-17, por pedido
entregado (96,470):

| Criterio | Entregas tardías | % |
| --- | --- | --- |
| Timestamps completos (ADR 0033) | 7,826 | 8.1% |
| Días calendario | 6,534 | 6.8% |
| Diferencia: entregados el día prometido | 1,292 | 1.3 pts |

Además, con fracción, un pedido entregado a las 18:00 del día prometido
tendría `dias_retraso = 0.75` y no sería tardío por día: dos columnas de la
misma fila se contradirían.

## Decisión

- **`dias_retraso`** = días calendario de la fecha estimada a la de entrega
  (`date_diff('day', ...)` sobre las fechas, no los timestamps). Entero
  (`BIGINT`): 0 si llegó el día prometido, negativo si llegó antes; nulo
  mientras no hay entrega.
- **`es_entrega_tardia`** = la fecha de entrega es posterior a la estimada,
  es decir `dias_retraso > 0`.
- `dias_hasta_aprobacion` y `dias_hasta_entrega` no cambian: miden
  duraciones entre dos instantes con hora, y ahí la fracción es real.

## Alternativas consideradas

- **Timestamps completos (ADR 0033):** consistente con el dato crudo, pero
  "tarde" pasa a significar "después del comienzo del día prometido", que
  no es lo que entiende nadie que lea el KPI.
- **Por día solo en `es_entrega_tardia`, con `dias_retraso` con fracción:**
  cambia menos, pero deja las dos columnas en contradicción el día
  prometido.
- **Asumir una hora de corte para la promesa** (fin del día, horario
  comercial): sería inventar un dato que Olist no trae.

## Por qué

Gold es la capa de consumo y se analiza en términos de negocio. La promesa
al cliente es un día, así que se cumple o se incumple por día. Una
comparación más precisa que el dato produce un KPI engañoso por defecto, el
mismo problema que llevó a `fct_reviews` a tener grano en `review_id` (ADR
0034).

## Consecuencias

- El KPI de entregas tardías del criterio de "hecho" de la Fase 4 pasa de
  8.1% a 6.8%. El promedio de días hasta la entrega (12.56) no cambia.
- El contrato de `fct_pedidos` cambia el tipo de `dias_retraso` de `DOUBLE`
  a `BIGINT`: hace falta `gold-build --full-refresh` sobre un Gold existente
  (`on_schema_change='fail'`, ADR 0029).
- El fixture de Gold incluye un pedido entregado a las 23:00 del día
  prometido (`g9`), que no cuenta como tardío.
- La conciliación de la Fase 4 reprodujo el 8.1% sin detectar el defecto:
  conciliar prueba que Gold es fiel a la definición, no que la definición
  sea correcta.
