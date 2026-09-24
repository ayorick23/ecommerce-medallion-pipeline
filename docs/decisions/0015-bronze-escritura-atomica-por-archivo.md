# 0015 — Escritura atómica en Bronze: reemplazo de archivo desde un área de staging

**Estado:** Aceptada
**Fecha:** 2026-09-24
**Supera parcialmente a:** [0012](0012-idempotencia-bronze-reemplazo-atomico-particion.md)
(solo el mecanismo de escritura; la semántica de reemplazo completo e
idempotencia sigue vigente)

## Contexto

La ADR 0012 decidió que reprocesar un día **reemplaza su partición
completa de forma atómica**, y describió el mecanismo así: escribir en un
directorio temporal hermano y reemplazar la partición final con un
renombrado. Al diseñar `src/bronze/storage.py` aparecieron dos problemas
con ese mecanismo:

- **Reemplazar un directorio existente no es atómico.** Ni en Windows ni
  en POSIX se puede renombrar un directorio sobre otro que ya existe y no
  está vacío. Hacen falta dos pasos (apartar el viejo y mover el nuevo),
  con un intervalo en el que el día no existe: justo lo que la 0012 quería
  evitar.
- **Un temporal dentro de la carpeta de la tabla rompe a los lectores.**
  Probado con Polars 1.43: si en `bronze/orders/` queda un archivo
  temporal huérfano (p. ej. de una corrida interrumpida), leer la tabla
  como directorio falla con `InvalidOperationError: directory contained
  paths with different file extensions`.

## Decisión

- Cada partición de eventos contiene **un solo archivo**
  (`<tabla>/dia_simulado=YYYY-MM-DD/part-0.parquet`), y cada tabla de
  referencia un solo `snapshot.parquet`. La atomicidad se garantiza a
  nivel de **archivo**, no de directorio.
- Escritura: el Parquet se escribe primero en un **área de staging** fuera
  de las carpetas de las tablas (`bronze/_staging/`), con un nombre único,
  y después se mueve a su destino final con `os.replace`, que reemplaza el
  archivo existente de forma atómica en Windows y en POSIX. Si la
  escritura falla, se borra el temporal y el destino no se toca.
- `dia_simulado` **no se guarda dentro de los archivos de partición**: lo
  da la ruta (particionado Hive) y los lectores lo reconstruyen como
  `Date`. En los snapshots, que no tienen ruta por día, sí va como columna.
- Día sin datos: se borra `part-0.parquet` (borrar un archivo es atómico)
  y después el directorio vacío de la partición.
- **Solo almacenamiento local en la Fase 2.** Si la raíz de almacenamiento
  es una URI remota (`abfs://...`), `storage.py` falla con un error
  explícito en lugar de escribir sin garantías de atomicidad.

## Alternativas consideradas

- **Directorio temporal hermano + dos renombrados** (ADR 0012 tal cual):
  no es atómico y deja temporales dentro de la carpeta de la tabla.
- **Temporal dentro de la carpeta de la partición**: el `os.replace` sería
  igual de atómico, pero un huérfano rompe la lectura de toda la tabla.
- **`dia_simulado` también dentro del archivo**: dato duplicado que puede
  contradecir a la ruta, y algunos lectores fallan si la columna existe en
  el archivo y en la ruta a la vez.

## Por qué

Un reemplazo de archivo es la operación atómica que los sistemas de
archivos sí garantizan; un reemplazo de directorio no. Como cada partición
tiene un solo archivo, reemplazar el archivo equivale a reemplazar la
partición, con la garantía real que la 0012 buscaba. Mantener el staging
fuera de las carpetas de las tablas asegura que un lector nunca ve un
archivo a medio escribir ni un huérfano.

## Consecuencias

- Una corrida interrumpida puede dejar huérfanos en `bronze/_staging/`.
  No afectan a ningún lector y se pueden borrar sin riesgo.
- Si una partición creciera tanto que conviniera dividirla en varios
  archivos, este mecanismo deja de alcanzar (varios `os.replace` no son
  atómicos en conjunto) y habría que revisarlo. A volumen de Olist no
  aplica: la partición diaria más grande de `orders` tiene 1,686 filas
  (2017-11-24, Black Friday).
- En los snapshots, `dia_simulado` e `ingested_at` significan "día y
  momento en que se escribió el contenido actual": como el snapshot no se
  reescribe si su `batch_hash` no cambia (ADR 0013), quedan fijos mientras
  la fuente no cambie.
- Habilitar Azure Blob (ADR 0008) requiere una nueva ADR con la estrategia
  de escritura para almacenamiento de objetos.
