# 0024 — Escritura de Silver: un archivo por tabla y un manifiesto al final

**Estado:** Aceptada
**Fecha:** 2026-09-25

## Contexto

Con la reconstrucción completa (ADR 0017), cada corrida reemplaza todas las
tablas de Silver. La escritura atómica por archivo de Bronze (ADR 0015)
garantiza que cada tabla quede entera, pero si el proceso se corta entre
una tabla y otra, Silver queda mezclado: unas tablas de D y otras de la
corrida anterior. También hay que decidir dónde queda el linaje: las
columnas de Bronze (`ingested_at`, `source_file`, `batch_hash`,
`dia_simulado`) no se pueden mantener fila a fila, porque una fila de
`silver_orders` junta varias re-emisiones de Bronze.

## Decisión

- **Layout** (relativo a la raíz de almacenamiento, ADR 0008), sin
  partición:

  ```text
  silver/
  ├── _staging/                          ← temporales de escritura
  ├── _manifest.json                     ← se escribe al final
  ├── _pendientes/order_reviews.parquet  ← ADR 0020
  └── <tabla>/part-0.parquet             ← una carpeta por tabla, sin el prefijo silver_
  ```

- **Orden de escritura:**
  1. Construir y validar todo en memoria (ADR 0023).
  2. Borrar `_manifest.json`: marca "Silver en construcción".
  3. Escribir cada tabla y el archivo de pendientes de forma atómica,
     reusando el mecanismo de la ADR 0015 (staging + `os.replace`), que se
     mueve de `medallion.bronze` a `medallion.common`.
  4. Escribir `_manifest.json` de forma atómica.
- **Manifiesto:** fecha D (`as_of`), momento de la corrida (`built_at`),
  rango y cantidad de días de Bronze leídos, filas por tabla y cantidad de
  reviews pendientes. Reemplaza al linaje por fila: Silver no tiene
  columnas de linaje.
- Solo almacenamiento local, como en Bronze (ADR 0015).

## Alternativas consideradas

- **Directorios versionados con un puntero a la versión vigente:**
  atómico de verdad entre tablas, pero los lectores tienen que resolver el
  puntero (los *sources* de dbt usan rutas fijas), hay que limpiar las
  versiones viejas y en Windows los enlaces simbólicos requieren permisos.
  En el fondo es reinventar Delta Lake/Iceberg, descartados en la ADR 0006.
- **Armar el directorio en staging y renombrarlo sobre el actual:** en
  Windows no se puede renombrar un directorio sobre otro existente; hay
  que borrar primero y queda una ventana sin Silver.
- **Particionar las tablas** (p. ej. por mes de compra): con la
  reconstrucción completa se reescribe todo igual, y Silver es chico.

## Por qué

Es simple, reusa código ya probado, y el manifiesto cumple tres funciones:
linaje de la corrida, marca de "corrida completa" y contrato con los
lectores. La inconsistencia entre tablas no se impide, pero se **detecta**,
y volver a correr D la repara porque la reconstrucción es idempotente.

## Consecuencias

- Si falta `_manifest.json`, o su `as_of` no es el esperado, Silver no se
  debe leer. dbt/Airflow lo verifican antes de construir Gold (Fases 4 y
  5).
- Una corrida interrumpida puede dejar huérfanos en `silver/_staging/`, sin
  efecto sobre los lectores (igual que en Bronze).
- Habilitar Azure Blob (ADR 0008) requiere revisar esta ADR junto con la
  0015.
