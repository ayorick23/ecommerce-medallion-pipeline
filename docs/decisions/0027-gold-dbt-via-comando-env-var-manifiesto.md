# 0027 — dbt se ejecuta a través de `gold-build` / `gold-dbt`: rutas por `env_var` y verificación del manifiesto de Silver

**Estado:** Aceptada — supera parcialmente a la 0007 (cómo el DAG invoca a
dbt y dónde viven los tests de Gold)
**Fecha:** 2026-09-28

## Contexto

dbt es una herramienta aparte: no lee `config/pipeline.yaml` ni conoce el
manifiesto de Silver. Para construir Gold necesita tres cosas que hoy solo
sabe el código Python:

1. **Dónde está Silver**, para leer los Parquet como *sources*.
2. **Dónde escribir Gold** (`warehouse.duckdb`, ADR 0005).
3. **Si Silver está completo y es del día D:** el manifiesto se escribe al
   final; si falta, Silver quedó a medias, y su `as_of` dice a qué D
   corresponde (ADR 0024).

La raíz real no es solo lo que dice el YAML: `PIPELINE_STORAGE_ROOT` la
sobrescribe (ADR 0008). Y sin la verificación del punto 3, dbt construiría
Gold sobre un Silver incompleto o de otro día, sin avisar; con hechos
incrementales (ADR 0029), la marca de agua avanzaría sobre datos
equivocados y el error quedaría guardado en Gold hasta un full refresh.

## Decisión

- **Rutas por variables de entorno sin valor por defecto.** `profiles.yml`
  y los *sources* usan `{{ env_var('MEDALLION_GOLD_DB') }}` y
  `{{ env_var('MEDALLION_SILVER_URI') }}`, sin segundo argumento. Correr
  `dbt` directamente, sin esas variables, falla al parsear el proyecto.
- **Dos comandos en `src/medallion/gold/`** que hacen `load_config()`,
  calculan las rutas con `layer_uri()`, definen las variables e invocan dbt
  en el mismo proceso con `dbtRunner` (API de Python de dbt), apuntando a
  `dbt/` como proyecto y como directorio de perfiles:
  - `uv run gold-build --dia AAAA-MM-DD [--full-refresh] [--config RUTA]`:
    el comando del pipeline. **Antes de llamar a dbt** verifica con
    `read_manifest` que el manifiesto de Silver exista y que su `as_of` sea
    D; si no, falla sin tocar Gold. Después construye Gold de forma
    atómica (ADR 0028).
  - `uv run gold-dbt <argumentos de dbt>`: para desarrollo. Solo define las
    variables y pasa los argumentos tal cual (`gold-dbt test -s
    dim_cliente`, `gold-dbt docs generate`). **No verifica el manifiesto**,
    a propósito: sirve para iterar sobre un modelo sin reconstruir Silver.
- **Nombres:** prefijo `MEDALLION_`, distinto de `PIPELINE_`. Las
  `PIPELINE_*` las define la persona para configurar; las `MEDALLION_*` las
  define el comando a partir de la configuración, y las pisa si ya
  existían.
- **Rutas absolutas:** la ruta de Silver se pasa resuelta a absoluta,
  porque las vistas de staging la guardan dentro de `warehouse.duckdb`
  (ADR 0026) y una ruta relativa dependería del directorio desde donde se
  abra el archivo.
- **Solo local:** si la ruta de Gold no es local, el comando falla. DuckDB
  no escribe su archivo sobre `abfs://`; leer Silver desde la nube sería
  posible más adelante, pero Azure está diferido a la Fase 7/8 (ADR 0008).

## Alternativas consideradas

- **Rutas escritas a mano en dbt** (`path: ../data/gold/warehouse.duckdb`):
  dbt corre solo, pero hay dos fuentes de verdad. Si alguien define
  `PIPELINE_STORAGE_ROOT`, Bronze y Silver se escriben en un lado y dbt lee
  el Silver viejo de `./data`, sin error.
- **Pasar las rutas con `--vars`:** `var()` no existe dentro de
  `profiles.yml`, así que la ruta de Gold igual necesitaría `env_var`: dos
  mecanismos para lo mismo.
- **Verificar el manifiesto en un hook `on-run-start`:** se puede (leer el
  JSON con `read_json` de DuckDB y cortar con `raise_compiler_error`), pero
  duplica en Jinja lo que ya hace `read_manifest`, no se prueba con pytest
  y reparte la lógica entre dos lenguajes.
- **`dbt source freshness`:** mide la antigüedad del dato ("¿se actualizó
  en las últimas 24 h?"), no si Silver está completo ni si es de D. Con
  fechas simuladas de 2018 no tiene sentido.
- **Un sensor de Airflow:** sirve para *esperar* a Silver, no reemplaza la
  verificación: Gold también se corre en local y en los tests. Puede
  sumarse en la Fase 5, además de esto.

## Por qué

Una sola fuente de verdad para las rutas, y el mismo principio que el
resto del pipeline: sin valores por defecto que escondan un error. La
verificación del manifiesto queda en una función Python pura, reusando
código probado, y es exactamente lo que Airflow va a llamar. dbt queda
limitado a lo suyo: transformar y validar.

## Consecuencias

- **Supera parcialmente a la ADR 0007** en dos puntos:
  - El DAG de la Fase 5 invoca `gold-build --dia D`, no `dbt build`:
    llamar a dbt directo se saltaría la verificación del manifiesto y la
    escritura atómica.
  - `tests/unit/gold/` vuelve a tener sentido: ahí se prueban los comandos
    (guardas, variables, reemplazo). Los tests de datos siguen en dbt
    (ADR 0036).
  El resto de la 0007 sigue vigente.
- Concreta la consecuencia de la ADR 0024 ("dbt/Airflow verifican el
  manifiesto antes de construir Gold"): quien verifica es el comando que
  envuelve a dbt.
- `dbt` ya no se corre "a pelo": los comandos de dbt se lanzan con
  `uv run gold-dbt ...`. Las herramientas que llaman a dbt por su cuenta
  (la extensión dbt Power User de VS Code, `dbt docs serve`) necesitan las
  variables definidas; se documenta en el README.
- `gold-dbt` escribe directo en `warehouse.duckdb`, sin la garantía de la
  ADR 0028: es una herramienta de desarrollo, no del pipeline.
- `gold-build` y `gold-dbt` se registran en `[project.scripts]`. Importan
  dbt, así que requieren el grupo `gold` instalado (ADR 0026).
