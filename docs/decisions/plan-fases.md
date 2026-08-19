# Plan por Fases — Pipeline de Data Engineering E-commerce (Portafolio, Proyecto 3)

> Documento vivo. Se actualiza al cierre de cada fase con lo realmente decidido
> (no solo lo planeado). Si una decisión cambia a mitad de camino, se registra
> aquí el motivo — eso también es parte de lo que un evaluador técnico quiere ver.
>
> Las convenciones de trabajo del repo (git, tooling, tests) están en
> `CLAUDE.md` en la raíz del proyecto — léelo antes de retomar cualquier fase.

## Cómo vamos a trabajar

Por cada fase: primero diseño y discusión (yo explico el concepto y las
alternativas, decidimos juntos), después código. No se escribe código de una
fase hasta que el diseño de esa fase esté cerrado. Al final de cada fase hay
una revisión conjunta antes de avanzar a la siguiente — un "code review"
pedagógico, no solo un checklist técnico.

## Principios rectores (aplican a todas las fases)

1. **Diseño antes que código.** Los esquemas y contratos de datos se definen
   en un documento antes de escribir la primera línea de transformación.
2. **Lógica de transformación desacoplada de Airflow.** Bronze/Silver/Gold se
   implementan como funciones Python puras (reciben datos, devuelven datos),
   testeables con pytest sin levantar Airflow. Airflow solo orquesta —
   los `PythonOperator`/`@task` son envoltorios delgados que llaman a esas
   funciones. Esto es estándar en la industria porque permite testear la
   lógica de negocio en segundos, no en minutos con un scheduler corriendo.
3. **Fail-fast real.** Ninguna capa avanza a la siguiente si la validación
   falla. Sin "seguir con warnings".
4. **Idempotencia.** Reprocesar una fecha ya procesada no duplica ni corrompe.
5. **Config externalizada.** Nada de rutas o parámetros hardcodeados; todo en
   YAML/`.env`.
6. **Decisiones documentadas.** Cada decisión de diseño no trivial se
   registra como una ADR independiente en `docs/decisions/` (una por
   archivo, numeración secuencial). Ver índice al final de este documento.

## Fases

### Fase 0 — Fundamentos y entorno de trabajo

**Objetivo pedagógico:** entender la anatomía de un proyecto de datos
productivo (vs. un notebook) y las diferencias clave entre Polars y pandas
(lazy evaluation, query plan, `collect()`).

**Entregables:**

- Estructura definitiva de carpetas (`src/`, `dags/`, `config/`, `tests/`, `docs/`).
- `pyproject.toml` con `uv`, dependencias base (polars, pandera, duckdb, pytest).
- Dataset Olist descargado y explorado (script/notebook exploratorio — **no**
  forma parte del pipeline final, es solo para entender las tablas y sus
  relaciones).
- Esqueleto de `docker-compose.yml` (Airflow webserver + scheduler + su
  metastore Postgres).

**Criterio de "hecho":** `docker compose up` levanta el Airflow UI, y puedes
explicar en tus palabras el modelo relacional de Olist (qué tabla es el
grano de un pedido, cómo se conectan pagos/reviews/items).

---

### Fase 1 — Diseño de datos y contratos (sin código de pipeline)

**Objetivo:** definir el "contrato" de cada capa antes de tocar código de
transformación. Esta es la fase más importante para demostrar pensamiento de
arquitecto, no solo capacidad de escribir código.

**Entregables:**

- `docs/schemas.md`: columnas, tipos, PK/FK de bronze/silver/gold.
- Estrategia de replay cronológico definida por escrito (qué timestamp de
  Olist determina el "día de llegada simulada", y qué pasa cuando un pedido
  cambia de estado en un día distinto al de su ingesta inicial).
- Reglas de calidad no negociables por transición de capa (lista explícita
  de qué dispara fail-fast).
- Diseño del SCD2 simplificado para el historial de estados de pedido.
- Diagrama ER del modelo Gold (star schema).

**Criterio de "hecho":** documento de diseño revisado por ambos, sin
ambigüedades, antes de escribir una función de transformación.

---

### Fase 2 — Capa Bronze: ingesta y simulación de llegada incremental

**Entregables:** módulo Python puro que, dada una "fecha simulada", ingesta
el subconjunto correspondiente de Olist a bronze, particionado por fecha,
con metadata de linaje (`ingested_at`, `source_file`, `batch_hash`).

**Aprendizaje:** particionado de datos, metadata de linaje, diseño idempotente.

**Criterio de "hecho":** correr el proceso para 3 fechas simuladas distintas
hace crecer bronze incrementalmente sin duplicar ni corromper datos previos;
reprocesar una fecha ya corrida da el mismo resultado (idempotencia probada,
no solo asumida).

---

### Fase 3 — Validación y capa Silver

**Entregables:** esquemas Pandera (bronze→silver), limpieza y normalización,
implementación real del SCD2 de estados de pedido, fail-fast real
(excepción controlada que detiene el DAG).

**Aprendizaje:** validación declarativa de datos, SCD2 en la práctica (no
solo en teoría).

---

### Fase 4 — Capa Gold: modelado dimensional

**Entregables:** `fct_pedidos`, `fct_pagos`, `dim_cliente`, `dim_producto`,
`dim_vendedor`, `dim_tiempo`, `dim_estado_pedido` cargadas en DuckDB;
validación Pandera silver→gold.

**Aprendizaje:** construcción de star schema real, estrategias de carga
(upsert/merge) en DuckDB.

---

### Fase 5 — Orquestación completa con Airflow

**Entregables:** DAG(s) que envuelven las funciones puras de las fases 2–4
como tasks, dependencias entre capas, política de reintentos, manejo de
fallos (y sensores si el diseño de Fase 1 los justifica).

**Aprendizaje:** Airflow real — operators, dependencias entre tasks, retries,
backfill. Si seguimos el principio de desacoplar lógica (ver principio #2),
esta fase es "conectar", no "reescribir".

---

### Fase 6 — Testing automatizado

**Entregables:** suite pytest para la lógica de bronze/silver/gold, con
casos borde explícitos: batch vacío, duplicados, columnas faltantes, tipos
inválidos, disparo de fail-fast.

**Aprendizaje:** testing de pipelines de datos con fixtures pequeños
(DataFrames Polars sintéticos), no "que corra sin error".

---

### Fase 7 — Contenerización y reproducibilidad end-to-end

**Entregables:** `docker-compose.yml` final, validado corriendo el pipeline
completo desde cero; variables de entorno documentadas; healthchecks.

**Aprendizaje:** reproducibilidad real — que otra persona pueda clonar el
repo y levantarlo sin ayuda tuya.

---

### Fase 8 — Documentación y narrativa de portafolio

**Entregables:** README completo (arquitectura, decisiones técnicas con su
porqué, diagrama, cómo correrlo, capturas del Airflow UI y de consultas
sobre Gold).

**Aprendizaje:** comunicar trabajo técnico a un evaluador que tiene 5
minutos para decidir si le interesa tu perfil.

---

## Decisiones abiertas — a resolver en Fase 1

1. **Granularidad del replay:** confirmado que es diario. Falta definir qué
   timestamp de Olist ancla el "día de llegada" de un pedido, y cómo entran
   a bronze las actualizaciones de estado que ocurren en días posteriores a
   la ingesta inicial del pedido (¿nuevo registro en bronze ese día? ¿upsert?).

## Índice de decisiones de arquitectura (ADR)

Cada decisión de diseño no trivial vive como archivo independiente en
`docs/decisions/`. Este índice se actualiza a medida que se agregan nuevas.

| #    | Título                                              | Estado   |
| ---- | ----------------------------------------------------- | -------- |
| [0001](decisions/0001-fail-fast-hard-stop-total.md)                 | Semántica de fail-fast: hard stop total            | Aceptada |
| [0002](decisions/0002-desacople-logica-transformacion-airflow.md)   | Desacoplar la lógica de transformación de Airflow  | Aceptada |
