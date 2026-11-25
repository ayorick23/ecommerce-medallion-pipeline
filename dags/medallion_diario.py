"""DAG ``medallion_diario``: Bronze → Silver → Gold, una corrida por día.

Diseño en las ADRs 0040 (calendario y reproceso), 0041 (venv aparte y contrato
por código de salida) y 0042 (alertas). Este archivo solo conecta piezas: la
lógica está en ``src/medallion`` (venv del pipeline) y en los módulos puros de
``medallion_dag``, que se prueban sin Airflow.
"""

import logging
import subprocess
from datetime import UTC, datetime, time
from typing import Any

from airflow import DAG
from airflow.exceptions import AirflowException, AirflowFailException
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email

from medallion_dag.config import OrchestrationConfig, load_orchestration_config
from medallion_dag.contract import (
    LAYERS,
    Outcome,
    alert_message,
    layer_command,
    outcome,
    pipeline_python,
)

log = logging.getLogger(__name__)

# Si falta algo, el DAG no carga y el error se ve en la UI (fail-fast, ADR 0042).
CONFIG = load_orchestration_config()


def run_layer(layer: str, dia: str, home: str) -> None:
    """Corre una capa para un día en el venv del pipeline y traduce su código de salida.

    El proceso corre desde la raíz del pipeline, como en el repo: dbt/ y las
    rutas relativas de pipeline.yaml se resuelven desde ahí.
    """
    command = layer_command(pipeline_python(home), layer, dia)
    log.info("Ejecutando: %s", " ".join(command))
    # La salida se pasa al log de la task línea a línea: el proceso hijo escribe
    # en su propio stdout, que Airflow no captura solo.
    with subprocess.Popen(
        command, cwd=home, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
    ) as process:
        assert process.stdout is not None
        for line in process.stdout:
            log.info(line.rstrip())
    result = outcome(process.returncode)
    if result is Outcome.FAIL:
        raise AirflowFailException(
            f"{layer}({dia}) falló por datos o configuración (código {process.returncode}); "
            "no se reintenta"
        )
    if result is Outcome.RETRY:
        raise AirflowException(f"{layer}({dia}) terminó con código {process.returncode}")


def _alert_callback(config: OrchestrationConfig) -> Any:
    if not config.alerts_enabled:
        return None

    def notify(context: dict[str, Any]) -> None:
        ti = context["task_instance"]
        subject, body = alert_message(
            ti.dag_id, ti.task_id, context["ds"], ti.try_number, ti.log_url
        )
        send_email(to=config.alert_email, subject=subject, html_content=body)

    return notify


def _midnight_utc(day: Any) -> datetime:
    return datetime.combine(day, time(), tzinfo=UTC)


with DAG(
    dag_id="medallion_diario",
    description="Bronze → Silver → Gold a la fecha D = logical_date (ADR 0040)",
    schedule="@daily",
    start_date=_midnight_utc(CONFIG.start_date),
    end_date=_midnight_utc(CONFIG.end_date),
    catchup=True,
    max_active_runs=1,
    default_args={
        "owner": "medallion",
        "depends_on_past": True,
        "wait_for_downstream": True,
        "retries": CONFIG.retries,
        "retry_delay": CONFIG.retry_delay,
        "on_failure_callback": _alert_callback(CONFIG),
    },
    tags=["medallion"],
) as dag:
    tasks = [
        PythonOperator(
            task_id=layer,
            python_callable=run_layer,
            op_kwargs={"layer": layer, "dia": "{{ ds }}", "home": CONFIG.pipeline_home},
        )
        for layer in LAYERS
    ]
    tasks[0] >> tasks[1] >> tasks[2]
