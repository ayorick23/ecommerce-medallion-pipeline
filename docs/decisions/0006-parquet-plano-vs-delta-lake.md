# 0006 — Parquet plano en lugar de un formato de tabla (Delta Lake / Iceberg)

**Estado:** Aceptada
**Fecha:** 2026-09-23

## Contexto

En la industria, los lakehouses no suelen guardar Parquet "suelto": usan
un **formato de tabla** encima de Parquet — Delta Lake (Databricks,
Microsoft Fabric) o Apache Iceberg (Snowflake, AWS, etc.). Estos formatos
agregan un log de transacciones que da escrituras atómicas (ACID),
`MERGE`/upsert, evolución de schema y *time travel*. Polars puede escribir
Delta vía `delta-rs`, así que técnicamente es viable en este proyecto.

## Decisión

Bronze y Silver se escriben como **Parquet plano**. La idempotencia se
implementa explícitamente en el código del pipeline (p. ej. sobrescribir
por completo la partición de un `dia_simulado` al reprocesarlo; mecanismo
exacto a definir en el diseño de la Fase 2), no delegada a un formato de
tabla.

## Alternativas consideradas

- **Delta Lake (vía `delta-rs`)**: más fiel a Databricks/Fabric; resuelve
  atomicidad y upsert "gratis". Descartado para este proyecto, no por
  inviable, sino por lo que esconde (ver "Por qué").
- **Apache Iceberg**: mismo razonamiento; además, el soporte de escritura
  desde Polars es menos maduro que el de Delta.

## Por qué

El objetivo pedagógico de la Fase 2 es entender y **probar** la
idempotencia (criterio de "hecho": reprocesar una fecha da el mismo
resultado). Con Parquet plano el pipeline tiene que resolverla a mano, lo
que deja claro qué problema resuelven Delta/Iceberg y por qué existen.
Además, a este volumen (~100k pedidos, un solo escritor) las garantías
extra de un formato de tabla no se aprovechan.

## Consecuencias

- Una escritura interrumpida puede dejar una partición a medio escribir:
  la estrategia de escritura de la Fase 2 debe contemplarlo (p. ej.
  escribir en un temporal y renombrar).
- Migrar a Delta Lake queda como extensión natural documentada en el
  README (Fase 8): el layout por capa/partición no cambia, solo el formato
  de escritura.
