"""Criterio de "hecho" de la Fase 5, probado con Airflow real (ADR 0040–0042).

Cada test arranca su propio Airflow en un contenedor (``conftest.py``) y
corre el DAG ``medallion_diario`` con ``airflow dags backfill`` sobre los 12
días del fixture de Gold (2017-04-03 → 2017-04-14).
"""

import email
import shutil
import subprocess
from datetime import timedelta
from email.message import EmailMessage
from email.policy import default
from pathlib import Path

import pytest

from medallion.gold.build import read_gold_build
from medallion.gold.compare import compare_warehouses
from medallion.silver.build import read_manifest
from tests.airflow.conftest import IMAGE_ENV, REPO, AirflowEnv, start_airflow
from tests.integration.gold.conftest import DAYS, FIRST_DAY, LAST_DAY, make_gold_env

pytestmark = pytest.mark.usefixtures("airflow_factory")


def _warehouse(data: Path) -> Path:
    return data / "gold" / "warehouse.duckdb"


def _reference_gold(root: Path) -> Path:
    """Gold construido con los comandos a mano, día por día: lo que Airflow debe igualar."""
    env = make_gold_env(root)
    for dia in DAYS:
        env.advance(dia, full_refresh=dia == FIRST_DAY)
    return env.warehouse


def _started(factory: list[AirflowEnv], env: AirflowEnv) -> AirflowEnv:
    factory.append(env)
    return env


def test_dag_in_real_airflow() -> None:
    """Propiedades del DAG, aislamiento de entornos y fail-fast de alertas (check_dag.py)."""
    import os

    result = subprocess.run(
        [
            "docker", "run", "--rm", "-e", "CONNECTION_CHECK_MAX_COUNT=0",
            "-v", f"{REPO / 'dags'}:/opt/airflow/dags:ro",
            "-v", f"{REPO / 'tests' / 'airflow'}:/checks:ro",
            os.environ[IMAGE_ENV], "python", "/checks/check_dag.py",
        ],
        capture_output=True,
        text=True,
        check=False,
    )  # fmt: skip
    assert result.returncode == 0, result.stdout + result.stderr


def test_each_run_leaves_the_pipeline_at_its_day_and_rewind_matches(
    airflow_factory: list[AirflowEnv], tmp_path: Path
) -> None:
    """Criterio 5 y 7: cada corrida deja Silver y Gold en su día, y *Clear + Future* de un
    día del medio deja Gold igual que los comandos a mano."""
    env = _started(airflow_factory, start_airflow(tmp_path / "airflow"))

    for dia in DAYS:
        result = env.backfill(dia, dia)
        assert result.returncode == 0, f"{dia}: {result.stdout[-3000:]}{result.stderr[-3000:]}"
        manifest = read_manifest(str(env.data / "silver"))
        gold = read_gold_build(_warehouse(env.data))
        assert manifest is not None and manifest["as_of"] == dia.isoformat(), dia
        assert gold is not None and gold["as_of"] == dia, dia

    reference = _reference_gold(tmp_path / "referencia")
    assert compare_warehouses(_warehouse(env.data), reference) == []

    # Rebobinar desde un día del medio: se vuelven a correr ese día y todos los siguientes.
    rewind = FIRST_DAY + timedelta(days=3)
    env.clear(rewind, LAST_DAY)
    result = env.backfill(rewind, LAST_DAY)
    assert result.returncode == 0, result.stdout[-3000:] + result.stderr[-3000:]

    # Gold retrocedió a propósito con full refresh y después avanzó en incremental.
    assert "publicado (full refresh)" in env.attempts(rewind, "gold")[-1]
    assert "publicado (incremental)" in env.attempts(rewind + timedelta(days=1), "gold")[-1]
    gold = read_gold_build(_warehouse(env.data))
    assert gold is not None and gold["as_of"] == LAST_DAY
    assert compare_warehouses(_warehouse(env.data), reference) == []


def _alert(mail: Path) -> EmailMessage:
    files = sorted(mail.glob("*.eml"))
    assert len(files) == 1, f"se esperaba un correo, hay {len(files)}"
    message = email.message_from_bytes(files[0].read_bytes(), policy=default)
    assert isinstance(message, EmailMessage)
    return message


def test_deterministic_failure_is_not_retried_and_stops_the_calendar(
    airflow_factory: list[AirflowEnv], tmp_path: Path
) -> None:
    """Criterio 6 y 8: un fail-fast de Silver no se reintenta, detiene los días
    siguientes y manda una sola alerta."""
    from tests.integration.gold.conftest import GOLD_CSV

    bad_day = FIRST_DAY + timedelta(days=2)  # compra de g4: llega su item con precio inválido
    items = GOLD_CSV["order_items"].replace(
        "g4,1,p1,s1,2017-04-08 00:00:00,15.00", "g4,1,p1,s1,2017-04-08 00:00:00,abc"
    )  # noqa: E501
    env = _started(
        airflow_factory,
        start_airflow(tmp_path / "airflow", alerts=True, overrides={"order_items": items}),
    )

    result = env.backfill(FIRST_DAY, bad_day + timedelta(days=1))
    assert result.returncode != 0

    assert env.states(bad_day - timedelta(days=1)) == {
        "bronze": "success",
        "silver": "success",
        "gold": "success",
    }
    assert env.states(bad_day)["silver"] == "failed"
    assert env.states(bad_day)["gold"] == "upstream_failed"
    silver_logs = env.attempts(bad_day, "silver")
    assert len(silver_logs) == 1, "un fallo determinista no se reintenta"
    assert "SilverValidationError" in silver_logs[0]
    # wait_for_downstream: el día siguiente no arranca mientras el anterior no termine bien.
    assert env.states(bad_day + timedelta(days=1)).get("bronze") != "success"

    # Silver quedó en el último día bueno: el fail-fast no escribió nada (ADR 0001).
    manifest = read_manifest(str(env.data / "silver"))
    assert manifest is not None and manifest["as_of"] == (bad_day - timedelta(days=1)).isoformat()

    alert = _alert(env.mail)
    assert alert["To"] == "alertas@example.com"
    assert f"medallion_diario.silver del {bad_day.isoformat()}" in str(alert["Subject"])


def test_transient_failure_is_retried_and_clear_resumes(
    airflow_factory: list[AirflowEnv], tmp_path: Path
) -> None:
    """Criterio 6: un fallo del entorno se reintenta; corregido el entorno, *Clear* sigue."""
    env = _started(airflow_factory, start_airflow(tmp_path / "airflow"))
    assert env.backfill(FIRST_DAY, FIRST_DAY).returncode == 0

    # Un archivo donde Gold necesita su carpeta temporal: OSError, código 1.
    staging = env.data / "gold" / "_staging"
    shutil.rmtree(staging, ignore_errors=True)
    staging.write_text("estorbo", encoding="utf-8")

    day = FIRST_DAY + timedelta(days=1)
    assert env.backfill(day, day).returncode != 0
    gold_logs = env.attempts(day, "gold")
    assert len(gold_logs) == 2, "un fallo transitorio se reintenta (retries: 1)"
    assert all("terminó con código 1" in log for log in gold_logs)
    assert env.states(day)["gold"] == "failed"

    staging.unlink()
    env.clear(day, day)
    assert env.backfill(day, day).returncode == 0
    gold = read_gold_build(_warehouse(env.data))
    assert gold is not None and gold["as_of"] == day
