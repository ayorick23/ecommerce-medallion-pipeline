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
2. **Lógica de transformación desacoplada de Airflow.** Bronze/Silver se
   implementan como funciones Python puras con Polars (reciben datos,
   devuelven datos), testeables con pytest sin levantar Airflow; Gold se
   implementa como un proyecto dbt sobre DuckDB (ADR 0007). Airflow solo
   orquesta — los `PythonOperator`/`@task` son envoltorios delgados que
   llaman a esas funciones o a `dbt build`. Esto es estándar en la industria
   porque permite testear la lógica de negocio en segundos, no en minutos
   con un scheduler corriendo.
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

**Diseño cerrado (2026-09-25):** Silver se reconstruye completo "a la fecha
D" (ADR 0017) y enmascara los eventos que todavía no ocurrieron (0018); el
SCD2 se ordena por etapa con `valid_from` que nunca retrocede (0019); las
reviews que llegan antes que su pedido esperan hasta 120 días (0020); el
contrato se ajustó con datos medidos (0021: PK de reviews, cuotas `>= 0`,
códigos postales como texto); montos en `Decimal(18,2)` (0022); todas las
fallas en una `SilverValidationError` (0023); un archivo por tabla con
manifiesto al final (0024). Contratos completos en `docs/schemas.md`,
secciones 3 a 5.

**Criterio de "hecho":** propiedades probadas, no asumidas:

1. `silver_build(D)` produce todas las tablas validadas con Pandera para
   cualquier D, y no escribe nada si algo falla.
2. Idempotencia: correr D dos veces da un contenido idéntico.
3. Sin datos del futuro: para varios D, Silver(D) no contiene ningún
   evento ni timestamp posterior a D.
4. Coherencia en el tiempo: si D1 < D2, el historial SCD2 de D1 es el
   comienzo del de D2 (solo cambia la fila vigente y se cierran sus
   `valid_to`).
5. Fail-fast probado: cada tipo de regla dispara `SilverValidationError`
   con un reporte correcto.
6. Reviews adelantadas: una pendiente aparece cuando llega su pedido, y
   una vencida frena el pipeline.
7. Corrida real conciliada: Silver del último día coincide con la fuente
   (99,441 pedidos, 112,650 items, 103,886 pagos, 99,224 reviews, 0
   pendientes), con el tiempo de corrida medido.
8. ruff, mypy, pre-commit y CI en verde; ADRs escritos; `schemas.md`
   actualizado; apuntes de la fase.

Diferido a la Fase 5: persistir el reporte de validación en JSON (ADR
0023).

---

### Fase 4 — Capa Gold: modelado dimensional

**Entregables:** proyecto dbt Core + `dbt-duckdb` (ADR 0007) que construye
`fct_pedidos`, `fct_pagos`, `dim_cliente`, `dim_producto`, `dim_vendedor`,
`dim_tiempo`, `dim_estado_pedido` en DuckDB leyendo Silver (Parquet) como
*sources*; validación silver→gold como tests de dbt (reglas de la sección 4
de `docs/schemas.md`).

> Cambio respecto del plan original (2026-09-23): la validación silver→gold
> era con Pandera y Gold se construía con Polars. Se pasó a dbt para cubrir
> el modelado dimensional con la herramienta estándar de la industria — ver
> ADR 0007.

**Aprendizaje:** construcción de star schema real, dbt (modelos, `ref()`,
sources, tests, docs y linaje), estrategias de materialización y carga
(tabla completa vs. incremental/merge) en DuckDB.

---

### Fase 5 — Orquestación completa con Airflow

**Entregables:** DAG(s) que envuelven las funciones puras de las fases 2–3
y el `dbt build` de la fase 4 como tasks, dependencias entre capas,
política de reintentos, manejo de fallos (y sensores si el diseño de Fase 1
los justifica). Puntos a resolver: aislar las dependencias de dbt de las de
Airflow, y serializar las escrituras a `warehouse.duckdb` (un solo escritor
— ADR 0005, 0007).

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

## Decisiones abiertas — resueltas en Fase 1

1. **Granularidad del replay:** diaria (confirmado). El timestamp que
   ancla el "día de llegada" de un pedido es `order_purchase_timestamp`
   (único sin nulos en el 100% de los pedidos). Las actualizaciones de
   estado en días posteriores generan un **nuevo registro** en Bronze ese
   día (re-emisión de la fila cruda, nunca upsert) — diseño completo en el
   ADR 0004 y en `docs/schemas.md`.

Ver `docs/schemas.md` para el documento de contratos completo (schemas de
las 3 capas, reglas de fail-fast, diseño SCD2, diagrama ER de Gold) —
cierre de la Fase 1.

## Stack tecnológico y flujo de datos (definido 2026-09-23, antes de Fase 2)

Cada herramienta cumple un rol; ninguna se solapa con otra.

| Rol | Herramienta | ADR |
| --- | --- | --- |
| Almacenamiento Bronze/Silver | Parquet plano (Bronze particionado por `dia_simulado`) | 0005, 0006 |
| Almacenamiento Gold (consumo) | DuckDB (`warehouse.duckdb`) | 0005 |
| Raíz de almacenamiento | Configurable: local por defecto; Azurite / Azure Blob opcionales | 0008 |
| Transformación Bronze/Silver | Polars (funciones puras en `src/`) | 0002, 0007 |
| Calidad Bronze→Silver | Pandera | 0001, 0003 |
| Transformación y calidad Silver→Gold | dbt Core + `dbt-duckdb` (modelos + tests) | 0007 |
| Orquestación | Apache Airflow (Azure Data Factory documentado como alternativa) | 0009 |
| Metastore de Airflow | Postgres (solo estado de Airflow, nunca datos del pipeline) | 0005 |
| Empaquetado / reproducibilidad | Docker Compose | — |

```text
data/raw/*.csv  (Olist, inmutable)
   │  Airflow → bronze_ingest(dia_simulado)          [Polars]
   ▼
data/bronze/<tabla>/dia_simulado=YYYY-MM-DD/*.parquet  (+ linaje)
   │  Airflow → silver_build                          [Polars + Pandera, fail-fast]
   ▼
data/silver/<tabla>/*.parquet
   │  Airflow → dbt build                             [dbt-duckdb, tests = fail-fast]
   ▼
data/gold/warehouse.duckdb  (star schema: dim_* / fct_*)
   │
   ▼
Consumo: SQL, notebooks, BI
```

## Índice de decisiones de arquitectura (ADR)

Cada decisión de diseño no trivial vive como archivo independiente en
`docs/decisions/`. Este índice se actualiza a medida que se agregan nuevas.

| # | Título | Estado |
| --- | --- | --- |
| [0001](0001-fail-fast-hard-stop-total.md) | Semántica de fail-fast: hard stop total | Aceptada |
| [0002](0002-desacople-logica-transformacion-airflow.md) | Desacoplar la lógica de transformación de Airflow | Aceptada — parcialmente superada por 0007 (Gold) |
| [0003](0003-alcance-fail-fast-normalizacion-vs-hard-stop.md) | Alcance del fail-fast: violaciones estructurales vs. normalización | Aceptada — parcialmente superada por 0021 (ejemplo de reviews) |
| [0004](0004-bronze-append-only-reemision-por-evento.md) | Bronze append-only con re-emisión por evento | Aceptada |
| [0005](0005-almacenamiento-por-capa-parquet-duckdb.md) | Almacenamiento por capa: Parquet en Bronze/Silver, DuckDB en Gold | Aceptada |
| [0006](0006-parquet-plano-vs-delta-lake.md) | Parquet plano en lugar de Delta Lake / Iceberg | Aceptada |
| [0007](0007-polars-bronze-silver-dbt-gold.md) | Polars en Bronze/Silver, dbt (dbt-duckdb) en Gold | Aceptada |
| [0008](0008-almacenamiento-configurable-local-azurite-azure.md) | Almacenamiento configurable: local, Azurite o Azure Blob | Aceptada |
| [0009](0009-airflow-como-orquestador-vs-adf.md) | Airflow como orquestador (ADF como alternativa) | Aceptada |
| [0010](0010-sin-dvc-versionamiento-datos.md) | No usar DVC para versionar datos | Aceptada |
| [0011](0011-bronze-schema-on-read-todo-texto.md) | Bronze schema-on-read: columnas fuente como texto | Aceptada |
| [0012](0012-idempotencia-bronze-reemplazo-atomico-particion.md) | Idempotencia en Bronze: reemplazo atómico de la partición | Aceptada — parcialmente superada por 0015 (mecanismo de escritura) |
| [0013](0013-batch-hash-contenido-canonico.md) | `batch_hash`: SHA-256 del contenido canónico del batch | Aceptada — parcialmente superada por 0014 (serialización) |
| [0014](0014-batch-hash-codificacion-prefijo-longitud.md) | `batch_hash`: codificación canónica con prefijo de longitud | Aceptada |
| [0015](0015-bronze-escritura-atomica-por-archivo.md) | Escritura atómica en Bronze: reemplazo de archivo desde staging | Aceptada |
| [0016](0016-proyecto-instalable-paquete-medallion.md) | Proyecto instalable como paquete `medallion` | Aceptada |
| [0017](0017-silver-reconstruccion-completa-a-la-fecha.md) | Silver se reconstruye completo "a la fecha D" | Aceptada |
| [0018](0018-silver-enmascarado-por-llegada.md) | Silver enmascara los eventos que todavía no ocurrieron | Aceptada |
| [0019](0019-scd2-orden-por-etapa-valid-from-monotonico.md) | SCD2: orden por etapa y `valid_from` que nunca retrocede | Aceptada |
| [0020](0020-early-arriving-reviews-plazo-de-gracia.md) | Reviews adelantadas: plazo de gracia y luego fail-fast | Aceptada |
| [0021](0021-ajustes-contrato-silver-datos-medidos.md) | Ajustes al contrato de Silver a partir de datos medidos | Aceptada |
| [0022](0022-montos-decimal.md) | Montos como `Decimal(18,2)` | Aceptada |
| [0023](0023-silver-validacion-excepcion-reporte.md) | Validación de Silver: todas las fallas en una `SilverValidationError` | Aceptada |
| [0024](0024-silver-escritura-manifiesto.md) | Escritura de Silver: un archivo por tabla y un manifiesto | Aceptada |
