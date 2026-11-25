from datetime import date, timedelta
from pathlib import Path

import pytest

from medallion.bronze.ingest import load_sources
from medallion.bronze.replay import source_day_range
from medallion.common.config import load_config
from medallion_dag.config import (
    ALERT_EMAIL_ENV,
    ALERTS_ENABLED_ENV,
    PIPELINE_PYTHON_ENV,
    SMTP_ENVS,
    OrchestrationConfigError,
    load_orchestration_config,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
REPO_CONFIG = REPO_ROOT / "config" / "pipeline.yaml"

SECTION = """\
orchestration:
  start_date: 2016-09-04
  end_date: 2018-10-17
  retries: 2
  retry_delay_minutes: 5
  alerts:
    enabled: {enabled}
"""
BASE_ENV = {PIPELINE_PYTHON_ENV: "/opt/medallion/.venv/bin/python"}
SECRETS = {name: "x" for name in (ALERT_EMAIL_ENV, *SMTP_ENVS)} | {
    ALERT_EMAIL_ENV: "yo@example.com"
}


def _write(tmp_path: Path, content: str) -> Path:
    path = tmp_path / "pipeline.yaml"
    path.write_text(content, encoding="utf-8")
    return path


def test_loads_section(tmp_path: Path) -> None:
    config = load_orchestration_config(_write(tmp_path, SECTION.format(enabled="false")), BASE_ENV)
    assert config.start_date == date(2016, 9, 4)
    assert config.end_date == date(2018, 10, 17)
    assert config.retries == 2
    assert config.retry_delay == timedelta(minutes=5)
    assert config.alerts_enabled is False
    assert config.alert_email is None
    assert config.pipeline_python == BASE_ENV[PIPELINE_PYTHON_ENV]


def test_repo_config_has_alerts_disabled() -> None:
    # Versionado en false: un clon del repo no manda alertas a nadie (ADR 0042).
    assert load_orchestration_config(REPO_CONFIG, BASE_ENV).alerts_enabled is False


@pytest.mark.skipif(
    not (REPO_ROOT / "data" / "raw").is_dir(), reason="sin el dataset de Olist en data/raw"
)
def test_repo_dates_match_source_range() -> None:
    # Las fechas del calendario se escriben a mano (el DAG no puede leer los CSV
    # al parsearse, ADR 0040): este test avisa si se desfasan de la fuente.
    config = load_orchestration_config(REPO_CONFIG, BASE_ENV)
    first, last = source_day_range(load_sources(load_config(REPO_CONFIG)))
    assert (config.start_date, config.end_date) == (first, last)


@pytest.mark.parametrize(
    "content",
    [
        "storage: {}\n",
        "orchestration: 3\n",
        "storage: [sin cerrar\n",
        SECTION.replace("2016-09-04", "'2016-09-04'"),
        SECTION.replace("2016-09-04", "2016-09-04 10:00:00"),
        SECTION.replace("2016-09-04", "2019-01-01"),
        SECTION.replace("retries: 2", "retries: -1"),
        SECTION.replace("retries: 2", "retries: true"),
        SECTION.replace("retry_delay_minutes: 5", "retry_delay_minutes: 0"),
        SECTION.replace("  retries: 2\n", ""),
        SECTION.format(enabled="'no'"),
        SECTION.replace("  alerts:\n    enabled: {enabled}\n", ""),
    ],
    ids=[
        "sin-seccion",
        "seccion-no-mapeo",
        "yaml-mal-formado",
        "fecha-como-texto",
        "fecha-con-hora",
        "inicio-despues-del-fin",
        "reintentos-negativos",
        "reintentos-booleano",
        "espera-cero",
        "sin-reintentos",
        "alertas-no-booleano",
        "sin-alertas",
    ],
)
def test_invalid_section_raises(tmp_path: Path, content: str) -> None:
    with pytest.raises(OrchestrationConfigError):
        load_orchestration_config(_write(tmp_path, content), BASE_ENV)


def test_missing_file_raises(tmp_path: Path) -> None:
    with pytest.raises(OrchestrationConfigError, match="No existe"):
        load_orchestration_config(tmp_path / "no.yaml", BASE_ENV)


def test_path_can_come_from_env(tmp_path: Path) -> None:
    path = _write(tmp_path, SECTION.format(enabled="false"))
    environ = {"PIPELINE_CONFIG": str(path), **BASE_ENV}
    assert load_orchestration_config(environ=environ).retries == 2


def test_env_enables_alerts_with_secrets(tmp_path: Path) -> None:
    path = _write(tmp_path, SECTION.format(enabled="false"))
    config = load_orchestration_config(path, {ALERTS_ENABLED_ENV: "true", **SECRETS, **BASE_ENV})
    assert config.alerts_enabled is True
    assert config.alert_email == "yo@example.com"


def test_env_can_disable_alerts(tmp_path: Path) -> None:
    path = _write(tmp_path, SECTION.format(enabled="true"))
    config = load_orchestration_config(path, {ALERTS_ENABLED_ENV: "false", **BASE_ENV})
    assert config.alerts_enabled is False


def test_env_flag_must_be_explicit(tmp_path: Path) -> None:
    path = _write(tmp_path, SECTION.format(enabled="false"))
    with pytest.raises(OrchestrationConfigError, match=ALERTS_ENABLED_ENV):
        load_orchestration_config(path, {ALERTS_ENABLED_ENV: "1", **BASE_ENV})


@pytest.mark.parametrize("missing", [ALERT_EMAIL_ENV, *SMTP_ENVS])
def test_alerts_without_a_secret_fail(tmp_path: Path, missing: str) -> None:
    # Fail-fast: el DAG no carga, en vez de fallar en silencio el día de la alerta.
    path = _write(tmp_path, SECTION.format(enabled="true"))
    secrets = {k: v for k, v in SECRETS.items() if k != missing} | BASE_ENV
    with pytest.raises(OrchestrationConfigError, match=missing):
        load_orchestration_config(path, secrets)


def test_pipeline_python_is_required(tmp_path: Path) -> None:
    path = _write(tmp_path, SECTION.format(enabled="false"))
    with pytest.raises(OrchestrationConfigError, match=PIPELINE_PYTHON_ENV):
        load_orchestration_config(path, {})
