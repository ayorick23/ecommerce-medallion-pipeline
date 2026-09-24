# 0012 — Idempotencia en Bronze: reemplazo atómico de la partición del día

**Estado:** Aceptada — parcialmente superada por
[0015](0015-bronze-escritura-atomica-por-archivo.md) en el mecanismo de
escritura (la semántica de reemplazo e idempotencia sigue vigente)
**Fecha:** 2026-09-23

## Contexto

El criterio de "hecho" de la Fase 2 exige que reprocesar un
`dia_simulado` ya ingerido dé el mismo resultado, sin duplicar ni
corromper. Como Bronze se escribe en Parquet plano (ADR 0006), no hay
formato de tabla que aporte transacciones: la idempotencia y la
atomicidad las tiene que garantizar el pipeline.

## Decisión

- La unidad de escritura es la **partición completa de un día** de una
  tabla de eventos (`<tabla>/dia_simulado=YYYY-MM-DD/`). Reprocesar un día
  **reemplaza** su partición entera; nunca se hace append sobre una
  partición existente.
- La escritura es **atómica**: se escribe primero en un directorio
  temporal hermano y, solo si la escritura termina bien, se reemplaza la
  partición final con un renombrado. Una corrida interrumpida deja, como
  mucho, un temporal huérfano, nunca una partición a medio escribir.
- Los snapshots de referencia siguen el mismo principio: el archivo se
  reemplaza completo, de forma atómica (ADR 0013 define cuándo se
  reescribe).
- **Definición de "mismo resultado"**: mismas filas (contenido de las
  columnas fuente) y mismo `batch_hash`. `ingested_at` queda **excluida**
  de la comparación: registra cuándo corrió la ingesta y por definición
  cambia en cada ejecución.

## Alternativas consideradas

- **Append y deduplicar al leer**: la lectura se vuelve más cara y la
  partición crece con cada reproceso.
- **Borrar la partición y escribir directo**: deja un intervalo en el que
  el día no existe o está a medias si el proceso se corta.
- **Saltar el día si ya existe**: no permite corregir un día si cambiaran
  la lógica o la fuente.

## Por qué

Reemplazar la partición es la forma más simple de razonar la idempotencia
("el día D en Bronze es siempre función del día D en la fuente") y es la
semántica de `INSERT OVERWRITE PARTITION` de los warehouses. El
escribir-y-renombrar es la técnica estándar para dar atomicidad sobre
sistemas de archivos.

## Consecuencias

- El renombrado es atómico en un sistema de archivos local. En
  almacenamiento de objetos (Azure Blob, ADR 0008) un "renombrado de
  directorio" no es atómico: si se habilita esa variante, la estrategia de
  escritura se revisa (queda anotado, no bloquea la Fase 2).
- La prueba de idempotencia compara el contenido sin `ingested_at` antes y
  después de reprocesar.
