"""Contrato entre el DAG y el venv del pipeline, sin Airflow (ADR 0041).

La task lanza ``python -m medallion.orchestration <capa> <dia>`` con el Python
del venv y decide qué hacer según el código de salida. Esa decisión vive aquí,
pura, para probarla con pytest; el DAG solo la traduce a excepciones de Airflow.
"""

from enum import Enum
from typing import Final

LAYERS: Final = ("bronze", "silver", "gold")
# Duplicado de medallion.orchestration.EXIT_DETERMINISTIC: el DAG no puede
# importar medallion. Un test verifica que los dos valgan lo mismo.
EXIT_DETERMINISTIC: Final = 3


class Outcome(Enum):
    SUCCESS = "success"
    FAIL = "fail"  # determinista: AirflowFailException, sin reintento
    RETRY = "retry"  # inesperado: excepción normal, Airflow reintenta


def pipeline_python(home: str) -> str:
    """El Python del venv que ``uv sync`` deja en la raíz del pipeline (imagen Linux)."""
    return f"{home.rstrip('/')}/.venv/bin/python"


def layer_command(python: str, layer: str, dia: str) -> list[str]:
    if layer not in LAYERS:
        raise ValueError(f"capa desconocida: {layer!r}")
    return [python, "-m", "medallion.orchestration", layer, dia]


def outcome(returncode: int) -> Outcome:
    if returncode == 0:
        return Outcome.SUCCESS
    if returncode == EXIT_DETERMINISTIC:
        return Outcome.FAIL
    return Outcome.RETRY


def alert_message(
    dag_id: str, task_id: str, dia: str, try_number: int, log_url: str
) -> tuple[str, str]:
    """Asunto y cuerpo HTML del correo de un fallo definitivo (ADR 0042).

    Solo metadatos de la corrida: nada de los datos del pipeline.
    """
    subject = f"[medallion] Falló {dag_id}.{task_id} del {dia}"
    body = (
        f"<p>La task <b>{task_id}</b> del DAG <b>{dag_id}</b> falló definitivamente "
        f"para el día <b>{dia}</b> (intento {try_number}).</p>"
        "<p>El calendario queda detenido en este día hasta que se corrija y se "
        "haga <i>Clear</i> de la task (ADR 0040).</p>"
        f'<p><a href="{log_url}">Ver el log</a></p>'
    )
    return subject, body
