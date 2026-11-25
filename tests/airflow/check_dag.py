"""Verifica el DAG en Airflow real, dentro de la imagen (ADR 0041).

No es un test de pytest: pytest no está en el entorno de Airflow, y Airflow no
está en el venv de uv. Se corre con el Python de Airflow::

    docker run --rm -e CONNECTION_CHECK_MAX_COUNT=0 \\
        -v ./dags:/opt/airflow/dags -v ./tests/airflow:/checks \\
        medallion-airflow:local python /checks/check_dag.py

Sale con 0 si todo se cumple; si no, un AssertionError dice qué falló.
"""

import os
import subprocess
import sys
from datetime import UTC, datetime, timedelta

DAGS = "/opt/airflow/dags"
HOME = os.environ["PIPELINE_HOME"]
VENV_PYTHON = f"{HOME}/.venv/bin/python"


def check_dag_properties() -> None:
    from airflow.models import DagBag

    bag = DagBag(dag_folder=DAGS, include_examples=False)
    assert not bag.import_errors, f"errores al importar: {bag.import_errors}"
    assert list(bag.dag_ids) == ["medallion_diario"], bag.dag_ids

    dag = bag.dags["medallion_diario"]  # get_dag() consultaría la base de metadatos
    assert dag.schedule_interval == "@daily"
    assert dag.catchup is True
    assert dag.max_active_runs == 1
    assert dag.start_date == datetime(2016, 9, 4, tzinfo=UTC), dag.start_date
    assert dag.end_date == datetime(2018, 10, 17, tzinfo=UTC), dag.end_date

    # Orden bronze → silver → gold, sin otras dependencias.
    upstream = {t.task_id: t.upstream_task_ids for t in dag.tasks}
    assert upstream == {"bronze": set(), "silver": {"bronze"}, "gold": {"silver"}}, upstream

    for task in dag.tasks:
        assert task.depends_on_past is True, task.task_id
        assert task.wait_for_downstream is True, task.task_id
        assert task.retries == 2, task.task_id
        assert task.retry_delay == timedelta(minutes=5), task.task_id
        # Alertas apagadas en el pipeline.yaml versionado (ADR 0042).
        assert not task.on_failure_callback, task.task_id
    print("DAG: propiedades OK")


def check_venv_isolation() -> None:
    """El venv tiene el pipeline y dbt, y no tiene Airflow; Airflow no tiene el pipeline."""
    pipeline = subprocess.run(
        [VENV_PYTHON, "-c", "import medallion.orchestration, dbt.cli.main"], check=False
    )
    assert pipeline.returncode == 0, "el venv no puede importar el pipeline o dbt"
    no_airflow = subprocess.run([VENV_PYTHON, "-c", "import airflow"], capture_output=True)
    assert no_airflow.returncode != 0, "Airflow está instalado en el venv del pipeline"
    no_medallion = subprocess.run([sys.executable, "-c", "import medallion"], capture_output=True)
    assert no_medallion.returncode != 0, "el pipeline está instalado en el entorno de Airflow"
    print("Aislamiento de entornos: OK")


def check_alerts_without_secrets_break_the_dag() -> None:
    """Con alertas activas y sin secretos, el DAG no carga (fail-fast, ADR 0042)."""
    code = (
        "from airflow.models import DagBag\n"
        f"bag = DagBag(dag_folder={DAGS!r}, include_examples=False)\n"
        "assert bag.import_errors, 'el DAG cargó sin los secretos de las alertas'\n"
        "assert 'PIPELINE_ALERT_EMAIL' in str(bag.import_errors), bag.import_errors\n"
    )
    env = {**os.environ, "PIPELINE_ALERTS_ENABLED": "true"}
    for name in list(env):
        if name == "PIPELINE_ALERT_EMAIL" or name.startswith("AIRFLOW__SMTP__"):
            del env[name]
    result = subprocess.run([sys.executable, "-c", code], env=env, check=False)
    assert result.returncode == 0, "faltan secretos y el DAG igual cargó"
    print("Alertas sin secretos: el DAG no carga, OK")


if __name__ == "__main__":
    check_dag_properties()
    check_venv_isolation()
    check_alerts_without_secrets_break_the_dag()
