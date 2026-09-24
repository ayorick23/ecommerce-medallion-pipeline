# 0011 — Bronze schema-on-read: todas las columnas fuente como texto

**Estado:** Aceptada
**Fecha:** 2026-09-23

## Contexto

`docs/schemas.md` lista los tipos de Bronze en términos conceptuales y deja
el tipo físico para la implementación. Al leer los CSV, Polars infiere
tipos por defecto (p. ej. los timestamps de `orders` como `Datetime`,
`review_score` como `Int64`).

La Fase 3 tiene una regla fail-fast explícita para **tipo no parseable**
en Bronze→Silver (sección 4 de `docs/schemas.md`). Si Bronze ya convierte
tipos al ingerir, un valor corrupto en la fuente se pierde antes de que
Silver pueda detectarlo: la inferencia lo deja nulo, lo degrada a otro
tipo, o hace fallar la ingesta con un error que no es de calidad de datos.

## Decisión

Bronze lee y persiste **todas las columnas fuente como texto** (`String`),
sin inferencia de tipos (*schema-on-read*). Las únicas columnas tipadas en
Bronze son las de linaje, que genera el propio pipeline:
`dia_simulado` (`Date`), `ingested_at` (`Datetime`), `source_file` y
`batch_hash` (`String`).

Para decidir a qué `dia_simulado` pertenece una fila, Bronze **parsea en
memoria** los timestamps ancla (ADR 0004) con un formato estricto, sin
modificar el valor que persiste. Si un timestamp ancla no nulo no se puede
parsear, la fila no se puede ubicar en el tiempo y la ingesta **falla**:
es un error estructural, no de calidad.

El tipado, la conversión y su validación son responsabilidad de Silver
(Fase 3).

## Alternativas consideradas

- **Tipos inferidos por Polars**: más cómodo aguas abajo, pero la
  inferencia depende de las primeras filas leídas y puede cambiar entre
  versiones, y esconde valores corruptos.
- **Schema explícito tipado en Bronze**: determinista, pero adelanta a
  Bronze una validación que el diseño asignó a Silver.

## Por qué

Bronze es la copia fiel de la fuente (ADR 0004). Guardar texto garantiza
que lo persistido es exactamente lo que llegó, y que cualquier problema de
tipo se detecta y reporta en el lugar diseñado para eso (Silver, con
Pandera). Es el patrón habitual de la capa "raw/bronze" en la industria.

## Consecuencias

- Silver hace todas las conversiones de tipo de forma explícita.
- `batch_hash` (ADR 0013) se calcula sobre texto crudo, lo que lo hace
  estable e independiente de la inferencia de tipos.
- Los nulos del CSV (campo vacío) se leen como nulos, no como `""`.
