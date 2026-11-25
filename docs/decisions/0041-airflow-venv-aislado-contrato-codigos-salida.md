# 0041 — Pipeline en un venv aparte de Airflow, con un contrato por código de salida

**Estado:** Aceptada — supera parcialmente a la
[0002](0002-desacople-logica-transformacion-airflow.md) (cómo invoca el DAG
la lógica: lanza un proceso en vez de importar las funciones)
**Fecha:** 2026-11-25

## Contexto

La imagen del compose (`apache/airflow:2.10.4-python3.12`) trae el
entorno de Airflow, con cientos de dependencias fijadas por su
*constraints file*. dbt-core también fija las suyas (Jinja, protobuf,
click…). Instalar el pipeline y dbt dentro del entorno de Airflow tiene
dos problemas:

1. Si resuelve, lo hace con versiones distintas a las de `uv.lock`: lo que
   corre en Airflow no es lo que probaron los tests y la CI.
2. Actualizar Airflow obliga a actualizar dbt, o al revés.

También hace falta reintentar solo lo que vale la pena. El código ya
separa dos tipos de fallo:

| Tipo | Ejemplos | ¿Reintentar? |
| --- | --- | --- |
| Determinista (datos o configuración) | `SilverValidationError`, `GoldBuildError`, `RoutingError`, `ConfigError`, `StorageError` | No: da el mismo error y retrasa la alerta |
| Transitorio (entorno) | disco lleno, contenedor reiniciado, `warehouse.duckdb` bloqueado por otro programa | Sí |

Airflow falla sin reintentos si la task lanza `AirflowFailException`. Pero
si la lógica corre en otro proceso (otro venv), Airflow solo ve "el proceso
terminó con error" y se pierde el tipo de excepción.

## Decisión

- **Imagen propia:** un `Dockerfile` `FROM apache/airflow:2.10.4-python3.12`
  instala el pipeline en un venv aparte, `/opt/medallion/.venv`, con `uv
  sync --locked --no-dev --group gold`. `--locked` hace fallar la
  construcción si `uv.lock` no está al día con `pyproject.toml`, en vez de
  resolver versiones nuevas. El entorno de Airflow no recibe nada del
  pipeline.
- **Un punto de entrada de orquestación en `src/`**, p. ej. `python -m
  medallion.orquestacion <capa> <dia>`, que corre una capa para un día y
  termina con un código que es el contrato con Airflow:

  | Código | Significado | Airflow |
  | --- | --- | --- |
  | 0 | Éxito | La task pasa |
  | 3 | Fallo determinista: una de las excepciones conocidas | `AirflowFailException`, sin reintento |
  | Cualquier otro (1 si escapa una excepción no prevista) | Fallo inesperado, posiblemente transitorio | Excepción normal, con reintentos |

  Ahí vive también el full refresh automático de Gold de la ADR 0040,
  que se prueba con pytest sin Airflow.
- **Las tasks son `@task` normales en el entorno de Airflow** (~15 líneas
  compartidas): lanzan el punto de entrada con el Python del venv y
  traducen el código. **El DAG no importa nada de `medallion`.**
- **Reintentos:** `orchestration.retries` (2) y
  `orchestration.retry_delay_minutes` (5) en `pipeline.yaml`, obligatorios
  y sin valor por defecto. Solo aplican a los fallos no deterministas.
- **Tests por entorno:** la lógica de `medallion.orquestacion` (códigos,
  full refresh, fechas de config) se prueba con pytest en el venv de
  `uv`, sin Airflow. Que el DAG cargue (`DagBag`) y una corrida real con
  `airflow dags test` se prueban dentro de la imagen, en un job aparte de
  la CI.

## Alternativas consideradas

- **Todo en el entorno de Airflow** (`pip install` en la imagen oficial):
  es lo más simple, pero tiene los dos problemas del contexto.
- **Un contenedor por task (`DockerOperator`):** es el máximo aislamiento,
  pero exige montar el socket de Docker dentro de Airflow: más piezas y un
  riesgo de seguridad. Tiene sentido con Kubernetes
  (`KubernetesPodOperator`), no en un compose local.
- **Astronomer Cosmos** (un modelo de dbt por task): choca con la ADR
  0028, que construye sobre una copia y publica de una vez. Con un modelo
  por task, Gold se publicaría a medias.
- **`@task.external_python`** con el intérprete del venv: fue la primera
  propuesta, porque llama a las funciones de `src/` al pie de la letra de
  la ADR 0002. Se descartó porque la función corre en otro proceso y
  Airflow pierde el tipo de excepción, así que no puede distinguir un
  fail-fast de un fallo transitorio.
- **`BashOperator`** que corre los comandos (`gold-build --dia {{ ds }}`):
  tiene el mismo problema (cualquier código distinto de 0 se reintenta), y
  la decisión de full refresh quedaría en un script de shell.

## Por qué

El aislamiento tiene que garantizar que en producción corren exactamente
las versiones que probó la CI, y eso lo da un venv construido desde el
lock. El código de salida es la única señal que cruza limpiamente entre
dos procesos con entornos distintos, y la separación entre fallos
esperados e inesperados ya existe en el código. Poner la lógica de
orquestación en `src/` mantiene el espíritu de la ADR 0002: el DAG queda
como un envoltorio delgado y lo que tiene reglas se prueba sin Airflow.

## Consecuencias

- La imagen hay que construirla (`docker compose build`): el compose deja
  de usar la imagen oficial tal cual. Adelanta parte de la Fase 7.
- Cada task arranca un intérprete más. Es poco frente a los ~10 s de
  Gold, y dbt ya corría en un proceso hijo (ADR 0037).
- El código 3 pasa a ser parte del contrato: una excepción conocida nueva
  hay que agregarla al punto de entrada, o se reintentará como si fuera
  transitoria (falla igual, solo que más tarde).
- Un error de argumentos (`argparse` sale con 2) se reintenta. Como el
  único que llama al punto de entrada es el DAG, se acepta.
- La CI suma un job con la imagen de Airflow.
