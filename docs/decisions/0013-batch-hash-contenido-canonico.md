# 0013 — `batch_hash`: SHA-256 del contenido canónico del batch

**Estado:** Aceptada
**Fecha:** 2026-09-23

## Contexto

`docs/schemas.md` define `batch_hash` como columna de linaje de Bronze
"para detectar reprocesos idénticos", sin fijar cómo se calcula. Además,
las tablas de referencia se cargan como snapshot completo (ADR 0004); la
mayor (`geolocation`, ~1M filas, 58 MB de CSV) se volvería a leer y
escribir en cada una de las ~691 corridas diarias del replay si no hay un
criterio para saltarla.

Polars ofrece `DataFrame.hash_rows()`, pero su documentación indica que el
resultado solo es estable dentro de una misma versión de Polars: un
`batch_hash` calculado así cambiaría al actualizar la librería, aunque los
datos fueran idénticos.

## Decisión

- `batch_hash` = **SHA-256** (hex) de una **serialización canónica** del
  batch:
  - solo columnas fuente (sin columnas de linaje), todas texto (ADR 0011),
    en el orden de columnas de la fuente;
  - filas **ordenadas** de forma determinista por todas las columnas, para
    que el orden de lectura no afecte al hash;
  - serializadas a CSV con una representación fija de los nulos.
- Mismo contenido ⇒ mismo `batch_hash`, independientemente del momento
  de ejecución y de la versión de Polars.
- **Tablas de referencia**: antes de reescribir un snapshot, se compara el
  `batch_hash` del contenido fuente con el del snapshot existente. Si
  coinciden, **no se reescribe** (se registra que no hubo cambios). Si
  difieren o no existe, se escribe de forma atómica (ADR 0012).

## Alternativas consideradas

- **`DataFrame.hash_rows()` de Polars**: rápido, pero no estable entre
  versiones.
- **Hash de los bytes del archivo Parquet escrito**: depende de la
  compresión, de metadatos y de la versión del escritor.
- **Hash del archivo CSV fuente completo** (solo para referencias): más
  barato, pero no aplica a las tablas de eventos, cuyo batch es un
  subconjunto filtrado del CSV. Un único criterio para ambos tipos de
  tabla es más fácil de explicar y testear.

## Por qué

Un hash sobre contenido canónico es reproducible y portable: sirve como
"huella" del batch en el linaje y como base para la prueba de
idempotencia (ADR 0012). Usarlo además para saltar snapshots sin cambios
le da a `batch_hash` una función real en el pipeline, no solo de metadata.

## Consecuencias

- El orden por todas las columnas tiene un costo en el snapshot más
  grande; es aceptable a este volumen y se mide al implementar.
- Calcular el hash exige leer el CSV fuente en cada corrida: lo que se
  ahorra es la **reescritura** del snapshot, no su lectura. Si la lectura
  resulta costosa en el replay completo, en la Fase 5 las referencias
  pueden ingerirse en una task que no corra por cada día.
- El `batch_hash` se guarda como columna (constante dentro del batch) en
  cada fila de la partición o snapshot, según `docs/schemas.md`.
