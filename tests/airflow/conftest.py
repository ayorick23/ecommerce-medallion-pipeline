"""Airflow real en un contenedor de la imagen, manejado desde pytest (ADR 0041).

Airflow no está en el venv de uv, así que estos tests no lo importan: lanzan
un contenedor de la imagen del compose y le hablan con ``docker exec``. Cada
escenario tiene su propio Airflow (SQLite, sin scheduler: ``airflow dags
backfill`` respeta ``depends_on_past`` y el orden), su ``pipeline.yaml`` y los
datos del fixture de Gold en un directorio temporal montado en ``/work``.

Se saltan si no está definida ``MEDALLION_AIRFLOW_IMAGE`` (la imagen ya
construida, p. ej. ``medallion-airflow:local`` después de ``docker compose
build``). La CI la define en el job ``airflow``.
"""

import json
import os
import subprocess
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pytest

from tests.integration.conftest import GRACE_DAYS, write_sources
from tests.integration.gold.conftest import FIRST_DAY, GOLD_CSV, LAST_DAY

IMAGE_ENV = "MEDALLION_AIRFLOW_IMAGE"
REPO = Path(__file__).resolve().parents[2]
DAG_ID = "medallion_diario"
SMTP_PORT = 8025

# Secretos de mentira para el SMTP de prueba (smtp_sink.py, sin TLS).
ALERT_ENV = {
    "PIPELINE_ALERT_EMAIL": "alertas@example.com",
    "AIRFLOW__SMTP__SMTP_HOST": "127.0.0.1",
    "AIRFLOW__SMTP__SMTP_PORT": str(SMTP_PORT),
    "AIRFLOW__SMTP__SMTP_USER": "prueba",
    "AIRFLOW__SMTP__SMTP_PASSWORD": "prueba",
    "AIRFLOW__SMTP__SMTP_MAIL_FROM": "airflow@example.com",
    "AIRFLOW__SMTP__SMTP_STARTTLS": "False",
    "AIRFLOW__SMTP__SMTP_SSL": "False",
}


def run_id(dia: date) -> str:
    return f"backfill__{dia.isoformat()}T00:00:00+00:00"


@dataclass
class AirflowEnv:
    """Un contenedor con Airflow listo y una raíz de datos montada en ``/work``."""

    name: str
    root: Path  # en el host; /work en el contenedor

    @property
    def data(self) -> Path:
        return self.root / "data"

    @property
    def mail(self) -> Path:
        return self.root / "mail"

    def exec(self, *command: str, detach: bool = False) -> subprocess.CompletedProcess[str]:
        flags = ["-d"] if detach else []
        return subprocess.run(
            ["docker", "exec", *flags, self.name, *command],
            capture_output=True,
            # Lo que sale del contenedor es UTF-8; en Windows, text=True solo usaría cp1252.
            encoding="utf-8",
            errors="replace",
            check=False,
        )

    def backfill(self, start: date, end: date) -> subprocess.CompletedProcess[str]:
        return self.exec(
            "airflow", "dags", "backfill", DAG_ID, "-s", start.isoformat(), "-e", end.isoformat()
        )

    def clear(self, start: date, end: date) -> None:
        """*Clear* de las tasks en el rango: lo que hace la UI con *Clear + Future*."""
        result = self.exec(
            "airflow", "tasks", "clear", DAG_ID, "-s", start.isoformat(), "-e", end.isoformat(),
            "-y",
        )  # fmt: skip
        assert result.returncode == 0, result.stderr

    def run_ids(self) -> set[str]:
        result = self.exec("airflow", "dags", "list-runs", "-d", DAG_ID, "-o", "json")
        assert result.returncode == 0, result.stderr
        return {row["run_id"] for row in json.loads(result.stdout or "[]")}

    def states(self, dia: date) -> dict[str, str | None]:
        """Estado de cada task de la corrida del día; vacío si la corrida no existe."""
        if run_id(dia) not in self.run_ids():
            return {}
        result = self.exec(
            "airflow", "tasks", "states-for-dag-run", DAG_ID, run_id(dia), "-o", "json"
        )
        assert result.returncode == 0, result.stderr
        return {row["task_id"]: row["state"] for row in json.loads(result.stdout)}

    def attempts(self, dia: date, task_id: str) -> list[str]:
        """Logs de cada intento de una task, en orden (``attempt=N.log``)."""
        folder = f"/opt/airflow/logs/dag_id={DAG_ID}/run_id={run_id(dia)}/task_id={task_id}"
        listing = self.exec("bash", "-c", f"ls '{folder}' 2>/dev/null | sort -V")
        return [
            self.exec("cat", f"{folder}/{name}").stdout for name in listing.stdout.split() if name
        ]


def _write_config(root: Path, *, alerts: bool) -> None:
    sources = "".join(f"  {table}: {table}.csv\n" for table in GOLD_CSV)
    (root / "pipeline.yaml").write_text(
        "storage:\n  root: /work/data\n"
        f"sources:\n{sources}"
        f"silver:\n  early_arriving_grace_days: {GRACE_DAYS}\n"
        "orchestration:\n"
        f"  start_date: {FIRST_DAY.isoformat()}\n"
        f"  end_date: {LAST_DAY.isoformat()}\n"
        # Un reintento con un minuto de espera: prueba el reintento sin alargar la CI.
        "  retries: 1\n"
        "  retry_delay_minutes: 1\n"
        f"  alerts:\n    enabled: {'true' if alerts else 'false'}\n",
        encoding="utf-8",
    )


def start_airflow(
    root: Path, *, alerts: bool = False, overrides: dict[str, str] | None = None
) -> AirflowEnv:
    image = os.environ[IMAGE_ENV]
    root.mkdir(parents=True, exist_ok=True)
    (root / "data").mkdir()
    write_sources(root / "data", {**GOLD_CSV, **(overrides or {})})
    _write_config(root, alerts=alerts)

    env = AirflowEnv(f"medallion-test-{uuid.uuid4().hex[:8]}", root)
    flags = ["-e", "CONNECTION_CHECK_MAX_COUNT=0", "-e", "PIPELINE_CONFIG=/work/pipeline.yaml"]
    for name, value in (ALERT_ENV if alerts else {}).items():
        flags += ["-e", f"{name}={value}"]
    if hasattr(os, "getuid"):  # Linux: que los archivos de /work sean del usuario del host
        flags += ["--user", f"{os.getuid()}:0"]
    subprocess.run(
        [
            "docker", "run", "-d", "--name", env.name, *flags,
            "-v", f"{root}:/work",
            "-v", f"{REPO / 'dags'}:/opt/airflow/dags:ro",
            "-v", f"{REPO / 'tests' / 'airflow'}:/checks:ro",
            image, "bash", "-c", "sleep infinity",
        ],
        check=True,
        capture_output=True,
    )  # fmt: skip
    for command in (
        ("airflow", "db", "migrate"),
        # Sin scheduler, nadie registra el DAG en la base; list-runs lo necesita.
        ("airflow", "dags", "reserialize"),
    ):
        result = env.exec(*command)
        assert result.returncode == 0, result.stderr
    if alerts:
        env.exec("python", "/checks/smtp_sink.py", "/work/mail", str(SMTP_PORT), detach=True)
    return env


@pytest.fixture
def airflow_factory(tmp_path: Path) -> Iterator[list[AirflowEnv]]:
    """Lista donde los tests registran sus contenedores; al final se borran todos."""
    if not os.environ.get(IMAGE_ENV):
        pytest.skip(f"{IMAGE_ENV} no está definida (imagen de Airflow del compose)")
    started: list[AirflowEnv] = []
    yield started
    for env in started:
        subprocess.run(["docker", "rm", "-f", env.name], capture_output=True, check=False)
