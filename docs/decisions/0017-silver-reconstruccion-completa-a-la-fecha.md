# 0017 — Silver se reconstruye completo "a la fecha D"

**Estado:** Aceptada
**Fecha:** 2026-09-25

## Contexto

Bronze crece un día a la vez (ADR 0004, 0012). Hay que decidir qué hace
`silver_build(D)` cuando corre para un día simulado D: procesar solo lo
que llegó ese día y mezclarlo con el Silver existente (incremental), o
reconstruir Silver entero a partir de todo Bronze hasta D.

A volumen de Olist, Bronze completo son 67.7 MB en 2,581 archivos Parquet
(replay verificado el 2026-09-24). Silver se escribe en Parquet plano (ADR
0005, 0006), que no tiene `MERGE`: actualizar una fila significa reescribir
el archivo que la contiene.

## Decisión

`silver_build(D)` es una **reconstrucción completa**: lee todas las
particiones de Bronze con `dia_simulado <= D` más los snapshots de
referencia, construye todas las tablas Silver desde cero y reemplaza las
anteriores. Silver(D) es una función pura de Bronze hasta D: no depende del
Silver de ninguna corrida previa.

## Alternativas consideradas

- **Incremental (merge):** leer solo la partición de D y hacer upsert sobre
  Silver, cerrando la fila vigente del SCD2. Costo constante por día y es
  el patrón de los sistemas grandes, pero Silver(D) pasa a depender de
  Silver(D-1): reprocesar un día viejo obliga a reprocesar todos los
  siguientes en orden, la idempotencia hay que construirla con merges por
  PK y los hechos que llegan antes que su padre necesitan una tabla de
  pendientes con estado propio. Sobre Parquet plano, sin `MERGE`, se
  reescribirían archivos completos igual: más complejidad sin ganar
  rendimiento real.
- **Híbrido** (incremental con reconstrucción completa periódica): suma la
  complejidad de ambas.

## Por qué

Sin estado propio, la idempotencia es automática (correr D dos veces da el
mismo resultado), un reproceso de cualquier día es simplemente volver a
correr ese día, y los casos que dependen del tiempo (hechos adelantados,
cierre de intervalos SCD2) se resuelven en cada corrida mirando todo, sin
ciclo de vida que mantener. El costo que crece con la historia es
irrelevante a este volumen. El patrón incremental/merge no se pierde como
aprendizaje: el plan lo ubica en la Fase 4, con los modelos incrementales
de dbt sobre DuckDB, donde sí hay `MERGE` nativo.

## Consecuencias

- No hace falta correr Silver para los 774 días del replay: Silver del
  último día es idéntico a lo que habría dejado la corrida diaria
  acumulada. Las propiedades temporales se prueban construyendo días
  elegidos (criterio de "hecho" de la Fase 3).
- Silver no puede distinguir "día sin datos en Bronze" de "día todavía no
  ingerido": Bronze no crea particiones vacías (ADR 0015). Que Bronze esté
  ingerido hasta D antes de construir Silver(D) lo garantiza la dependencia
  entre tareas en Airflow (Fase 5).
- El tiempo de una corrida crece con la historia; se mide en la corrida
  real de la Fase 3. Si algún día Bronze fuera mucho más grande, esta ADR
  es la que habría que revisar.
