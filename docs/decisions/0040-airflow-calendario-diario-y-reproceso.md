# 0040 — Airflow como calendario diario: una corrida por día y reproceso hacia adelante

**Estado:** Aceptada
**Fecha:** 2026-11-25

## Contexto

Los tres comandos del pipeline ya trabajan por día: `bronze-replay
--desde/--hasta`, `silver-build --dia D` y `gold-build --dia D`. Falta
decidir cómo los conecta Airflow con el tiempo.

Dos hechos condicionan la decisión:

- **Las capas no guardan el mismo tipo de estado.** Bronze está
  particionado por `dia_simulado` y volver a escribir un día reemplaza solo
  esa partición (ADR 0012). Silver y Gold, en cambio, son **una sola
  foto** a la fecha de la última corrida: Silver se reconstruye completo
  hasta D (ADR 0017) y Gold guarda su fecha en `_gold_build` (ADR 0028).
- **Gold no retrocede:** `gold-build` rechaza un D anterior al de
  `_gold_build` salvo con `--full-refresh`, porque los hechos incrementales
  no saben ir hacia atrás (ADR 0028, 0029).

Además, el orden importa, no solo que se corra de a una. Si `bronze(100)`
falla y el día 101 avanza igual, Silver(101) se construye sin los eventos
del 100 y Gold mueve su marca de agua más allá del 100: cuando se corrija
Bronze, los hechos incrementales ya no recogen esas filas. Todo queda en
verde y está mal, lo contrario del fail-fast (ADR 0001).

## Decisión

- **Un DAG, `medallion_diario`, con tres tasks en fila:** `bronze(D) →
  silver(D) → gold(D)`.
- **Airflow es el reloj:** `schedule="@daily"` y `catchup=True`. Cada
  corrida procesa el día `D = logical_date` (`{{ ds }}`). El replay
  completo de Olist es el catchup del DAG, y reprocesar un rango es un
  backfill.
- **Fechas del calendario en configuración:** `orchestration.start_date` y
  `orchestration.end_date` en `config/pipeline.yaml`, obligatorias y sin
  valor por defecto (2016-09-04 y 2018-10-17 para Olist). No se calculan
  leyendo los CSV porque Airflow vuelve a leer el archivo del DAG cada
  ~30 s. Un test compara esas fechas con `source_day_range()` para que no
  se desincronicen.
- **Orden estricto y un solo escritor**, con las tres perillas:
  - `max_active_runs=1`: una corrida a la vez, para que no haya dos
    escritores sobre los mismos temporales (`gold/_staging/build/` es una
    ruta fija, ADR 0028).
  - `depends_on_past=True`: una task del día D+1 corre solo si la misma
    task del día D terminó bien.
  - `wait_for_downstream=True`: además, espera a que terminen bien las
    tasks posteriores del día D (`bronze(101)` no arranca si `gold(100)`
    falló).

  Un fallo **detiene el calendario** en ese día hasta que alguien lo
  corrige.
- **Volver a correr un día pasado rebobina y vuelve a avanzar:** se hace
  *Clear* del día D con la opción *Future*, y Airflow vuelve a correr D y
  todos los siguientes, en orden. Para que Gold acepte el retroceso, el
  punto de entrada de orquestación (ADR 0041) corre `gold` con
  `--full-refresh` cuando D no es posterior al `as_of` de `_gold_build`.
  La regla resultante: **después de la corrida D, todo el pipeline está en
  D.**
- **Con Airflow activo, Airflow es el único escritor de su
  `storage.root`.** Para desarrollar con los comandos a mano se usa otra
  raíz (`PIPELINE_STORAGE_ROOT`). Queda documentado, no se hace cumplir
  con código.

## Alternativas consideradas

- **Airflow como botón:** un DAG sin schedule, disparado a mano con
  `desde`/`hasta`, que corre todo el rango en una corrida. Es más simple,
  pero Airflow ve una sola caja: si falla el día 400, falla todo, no hay
  estado por día y el backfill no significa nada. Además, alguien tiene
  que acordarse de dispararlo.
- **Un DAG por capa, unidos por Datasets:** conviene cuando las capas
  tienen dueños o ritmos distintos. Aquí las tres van al mismo ritmo y
  dependen día a día de la anterior: separarlas solo complica coordinar el
  orden.
- **"Solo avanzar":** las corridas de días pasados tocan solo Bronze, y
  Silver y Gold se saltan si D no es el día más reciente. Para aplicar una
  corrección se reconstruye a la fecha actual con `--full-refresh`. Es
  mucho más barato (una sola reconstrucción en vez de cientos de días),
  pero la corrida de D no deja el pipeline en D, y hace falta un camino
  especial que alguien tiene que recordar.
- **Solo `max_active_runs=1`:** evita los escritores simultáneos, pero si
  un día falla, el siguiente corre igual (el error silencioso del
  contexto).
- **Un pool de un slot para el warehouse:** limita la concurrencia entre
  DAGs distintos. Con un solo DAG no agrega nada a `max_active_runs=1`. Se
  agrega si aparece un segundo DAG que escriba.

## Por qué

Una corrida por día es lo que Airflow sabe manejar: estado por día,
reintentos por día y backfill. Rebobinar y volver a avanzar es la
semántica estándar de *Clear + Future* y da una regla que se razona sin
casos especiales. El full refresh automático evita a propósito la guarda
de Gold. La guarda existe para impedir que alguien retroceda **por
accidente**. Un *Clear* es retroceder a propósito, y por eso la decisión
vive en el punto de entrada de orquestación y `gold-build` no cambia.

## Consecuencias

- Corregir un día viejo cuesta volver a correr todos los siguientes.
  Estimado con los tiempos de la Fase 4 (~1 s Bronze, ~1.5 s Silver, ~10 s
  Gold por día), más el overhead de Airflow: unos 20 s por día. El
  catchup completo (774 días) tomaría unas 4 h, y rebobinar desde marzo de
  2017, unas 3 h. A este volumen es aceptable. Si el histórico creciera,
  "solo avanzar" es la alternativa documentada.
- Silver se reconstruye completo en cada corrida (ADR 0017). Es lo que
  hace posible rebobinar sin estado previo, pero cada día cuesta lo mismo
  que una reconstrucción.
- Un fallo deja el DAG detenido hasta que alguien actúa. Para enterarse
  sin mirar la UI están las alertas (ADR 0042).
- Hay dos fechas más en `pipeline.yaml` que mantener al cambiar de fuente.
  El test contra `source_day_range()` avisa si quedan desfasadas.
