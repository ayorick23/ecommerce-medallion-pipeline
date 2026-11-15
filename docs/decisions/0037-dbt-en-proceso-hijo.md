# 0037 — dbt se ejecuta en un proceso hijo, no con `dbtRunner`

**Estado:** Aceptada — supera parcialmente a la
[0027](0027-gold-dbt-via-comando-env-var-manifiesto.md) (cómo `gold-build` y
`gold-dbt` invocan a dbt; las rutas por `env_var` y la verificación del
manifiesto siguen vigentes)
**Fecha:** 2026-11-15

## Contexto

La ADR 0027 decidió que `gold-build` y `gold-dbt` invocaran dbt **en el
mismo proceso**, con `dbtRunner`, la API de Python de dbt.

Al correr la ventana real de la ADR 0036 (incremental día a día en un solo
proceso de Python), el segundo `gold_build` falló al abrir
`warehouse.duckdb` con "el proceso no tiene acceso al archivo porque está
siendo utilizado por otro proceso". El proceso que lo tenía abierto era el
mismo.

La causa está en dbt-duckdb (1.11): `DuckDBConnectionManager` guarda la
base abierta en un atributo **de clase** (`_ENV`), que sobrevive al final
de la corrida. Su `close_all_connections()` no la cierra: solo suelta la
referencia y deja el cierre al recolector de basura.

Con `uv run gold-build` no pasaba, porque cada día es un proceso nuevo.
Los tests unitarios tampoco lo detectaban, porque reemplazan a dbt por un
doble. Sí afecta a todo lo que llama a `gold_build` varias veces en el
mismo proceso: el fixture de integración de Gold, la ventana real y,
posiblemente, Airflow en la Fase 5.

Había un segundo efecto, menos visible: `run_dbt` hacía
`os.environ.update(env)`, así que las variables `MEDALLION_*` quedaban
definidas en el proceso que llamaba, después de la corrida.

## Decisión

`run_dbt` ejecuta dbt en un **proceso hijo**:

```python
subprocess.run(
    [
        sys.executable,
        "-c",
        "from dbt.cli.main import cli; cli()",
        *args,
        "--project-dir",
        p,
        "--profiles-dir",
        p,
    ],
    env={**os.environ, **env},
)
```

- Usa el mismo intérprete (`sys.executable`), así que usa el dbt del
  entorno de `uv` sin depender del `PATH`. Llama a `dbt.cli.main:cli`, el
  mismo punto de entrada del script `dbt`; `-m dbt.cli.main` también
  funciona, pero dispara un `RuntimeWarning` de `runpy`.
- El resultado es el código de salida: 0 es éxito, igual que `result.success`.
- Las variables `MEDALLION_*` pisan a las existentes **solo en el entorno
  del hijo**; el del proceso que llama no cambia.
- La salida de dbt va directo a la consola, como antes.

## Alternativas consideradas

- **Seguir con `dbtRunner` y cerrar `_ENV` a mano después de cada
  corrida:** es más rápido (no arranca otro intérprete), pero depende de un
  atributo privado de dbt-duckdb. Si el paquete cambia ese detalle, el bug
  vuelve sin que nada avise. El propio código del adaptador comenta que ese
  ciclo de vida es frágil.
- **Dejarlo como está y correr cada `gold_build` de los tests en un
  proceso aparte:** esconde el problema en los tests y lo deja en el
  código, para que lo encuentre Airflow.

## Por qué

Un proceso hijo da un límite que el sistema operativo garantiza: cuando
termina, todos sus archivos quedan cerrados, haga lo que haga la librería
por dentro. No depende de detalles internos de dbt-duckdb y además aísla a
dbt del proceso que lo llama, que es uno de los puntos que la Fase 5 tenía
que resolver con Airflow (dependencias y estado de dbt separados de los del
orquestador).

## Consecuencias

- Cada corrida suma el arranque de un intérprete e importar dbt (unos 2 a
  3 s), contra los ~20 s de un `gold-build` sobre Olist completo.
- Se pierde el objeto de resultado de `dbtRunner` (resultados por nodo).
  No se usaba: alcanza con el código de salida, y el detalle queda en
  `dbt/target/run_results.json`.
- Los tests unitarios de `run_dbt` reemplazan `subprocess.run` y verifican
  el comando, el entorno del hijo y que el del proceso no cambie.
- Que `gold_build` funcione varias veces en un mismo proceso lo prueban el
  fixture de integración de Gold y la ventana real (ADR 0036).
