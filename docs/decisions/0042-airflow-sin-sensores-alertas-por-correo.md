# 0042 — Sin sensores; alertas por correo activadas por despliegue

**Estado:** Aceptada
**Fecha:** 2026-11-25

## Contexto

El plan de la Fase 5 pide sensores "si el diseño de Fase 1 los justifica".
Un sensor es una task que espera algo externo: que aparezca un archivo,
que termine otro DAG. En este pipeline, `data/raw/` es un CSV estático y
el replay simula la llegada filtrando por fecha. No se espera nada que
venga de afuera.

Con la ADR 0040, un fallo detiene el calendario hasta que alguien actúa.
Sin un aviso, el DAG puede quedar detenido días sin que nadie lo note.
Pero el repo es público: si el destino de la alerta queda escrito en el
código, quien lo clone manda alertas de sus pruebas a ese destino, y la
dirección queda publicada en GitHub.

## Decisión

- **Sin sensores.** Si en la nube cada día llegara un archivo a Blob
  Storage desde un sistema externo, `bronze(D)` iría precedida por un
  sensor de ese archivo con `mode="reschedule"`, para no ocupar un slot
  mientras espera.
- **Alertas por correo (SMTP)** cuando una task falla **definitivamente**,
  después de los reintentos (`on_failure_callback`, no en cada
  reintento). El correo lleva el DAG, la task, el día D, el número de
  intento y el enlace al log. No lleva datos del pipeline.
- **El mecanismo va en el repo y el destino es de cada despliegue:**
  - `orchestration.alerts.enabled` en `pipeline.yaml`, obligatorio como
    todo parámetro y versionado en `false`. Se activa por despliegue con
    `PIPELINE_ALERTS_ENABLED=true`, igual que `PIPELINE_STORAGE_ROOT`
    sobrescribe la raíz.
  - El destino y las credenciales son secretos en `.env` (fuera de git):
    `PIPELINE_ALERT_EMAIL` y el SMTP de Airflow (`AIRFLOW__SMTP__*`, p.
    ej. Gmail con contraseña de aplicación). En el repo se versiona un
    `.env.example` con las variables vacías.
  - **Fail-fast:** si las alertas están activas y falta un secreto, el
    DAG no carga (error visible en la UI), en vez de fallar en silencio
    el día que haga falta avisar.

## Alternativas consideradas

- **Sin alertas, solo la UI:** el DAG en rojo y el calendario detenido.
  Alcanza si alguien mira la UI todos los días, que es justo lo que un
  orquestador debería evitar.
- **`email_on_failure` con la dirección en `default_args`:** es lo
  integrado de Airflow, pero la dirección queda en el código: datos
  personales en un repo público y alertas desde clones ajenos.
- **Teams (Workflows):** los *Incoming Webhooks* de Teams se están
  retirando, y su reemplazo suele requerir una cuenta de trabajo o
  estudiante de Microsoft 365. SMTP funciona con cualquier cuenta.
- **Webhook genérico** (Slack, Discord, ntfy, Teams): es más flexible,
  pero cada servicio espera un formato de mensaje distinto.
- **Activar las alertas solo por la presencia del secreto:** es más
  simple, pero "si no hay variable, no se alerta" es un valor por defecto
  implícito, contra la regla de configuración del repo. Además, un
  secreto mal escrito apaga las alertas sin avisar.

## Por qué

Un sensor solo tiene sentido cuando hay algo externo que esperar, y aquí
la fuente es local y fija. La alerta separa el mecanismo (código,
revisable y probado) del destino (secreto de quien despliega). Así el
repo no contiene datos personales y un clone no alerta a nadie. Activarla
con un flag explícito, que falla si está incompleto, es el mismo criterio
de fail-fast del resto del pipeline.

## Consecuencias

- Hay que configurar el SMTP de Airflow para recibir alertas. Con Gmail,
  una contraseña de aplicación (requiere verificación en dos pasos).
- El enlace al log apunta a `localhost:8080`: sirve en la máquina donde
  corre Airflow, no desde el teléfono.
- El callback se prueba sin enviar correos: con las alertas desactivadas
  no se llama a SMTP, y con un SMTP simulado se verifica el contenido.
- Si el pipeline pasa a la nube (Fase 7/8), aquí se agregarían el sensor
  y un canal más.
