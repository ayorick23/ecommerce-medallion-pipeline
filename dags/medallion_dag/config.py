"""Configuración de orquestación que lee el DAG (ADR 0040, 0041, 0042).

Solo usa ``yaml`` y la biblioteca estándar: no importa Airflow ni
``medallion``, así que corre en el entorno de Airflow y se prueba con pytest
en el venv del pipeline. Lee la sección ``orchestration`` de
``config/pipeline.yaml``; el resto del archivo lo valida ``medallion``.
"""

import os
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Final

import yaml

DEFAULT_CONFIG_PATH: Final = Path("config/pipeline.yaml")
CONFIG_PATH_ENV: Final = "PIPELINE_CONFIG"
ALERTS_ENABLED_ENV: Final = "PIPELINE_ALERTS_ENABLED"
ALERT_EMAIL_ENV: Final = "PIPELINE_ALERT_EMAIL"
# Lo que Airflow necesita para mandar correo. Si faltara alguno, Airflow usaría
# su valor por defecto (localhost:25) y la alerta fallaría el día que haga falta.
SMTP_ENVS: Final = (
    "AIRFLOW__SMTP__SMTP_HOST",
    "AIRFLOW__SMTP__SMTP_PORT",
    "AIRFLOW__SMTP__SMTP_USER",
    "AIRFLOW__SMTP__SMTP_PASSWORD",
    "AIRFLOW__SMTP__SMTP_MAIL_FROM",
)


class OrchestrationConfigError(ValueError):
    """La sección ``orchestration`` falta, está incompleta o faltan sus secretos."""


@dataclass(frozen=True)
class OrchestrationConfig:
    start_date: date
    end_date: date
    retries: int
    retry_delay: timedelta
    alerts_enabled: bool
    # Solo con las alertas activas; viene de .env, nunca del YAML versionado.
    alert_email: str | None


def _int_at_least(section: Mapping[str, Any], key: str, minimum: int) -> int:
    value = section.get(key)
    # bool es subclase de int en Python: `true` no debe pasar como 1.
    if not isinstance(value, int) or isinstance(value, bool) or value < minimum:
        raise OrchestrationConfigError(f"falta 'orchestration.{key}' (entero >= {minimum})")
    return value


def _date(section: Mapping[str, Any], key: str) -> date:
    value = section.get(key)
    # YAML convierte 2016-09-04 (sin comillas) en date; con comillas sería texto,
    # y con hora, datetime (que también es date en Python).
    if not isinstance(value, date) or isinstance(value, datetime):
        raise OrchestrationConfigError(
            f"falta 'orchestration.{key}' (fecha AAAA-MM-DD, sin comillas)"
        )
    return value


def _alerts_enabled(section: Mapping[str, Any], environ: Mapping[str, str]) -> bool:
    override = environ.get(ALERTS_ENABLED_ENV)
    if override is not None:
        if override not in ("true", "false"):
            raise OrchestrationConfigError(
                f"{ALERTS_ENABLED_ENV} debe ser 'true' o 'false', no {override!r}"
            )
        return override == "true"
    alerts = section.get("alerts")
    enabled = alerts.get("enabled") if isinstance(alerts, dict) else None
    if not isinstance(enabled, bool):
        raise OrchestrationConfigError("falta 'orchestration.alerts.enabled' (true/false)")
    return enabled


def load_orchestration_config(
    path: str | Path | None = None, environ: Mapping[str, str] | None = None
) -> OrchestrationConfig:
    """Lee y valida ``orchestration``; con las alertas activas, exige sus secretos.

    Ruta: argumento ``path`` → ``PIPELINE_CONFIG`` → ``config/pipeline.yaml``,
    igual que ``medallion.common.config.load_config``.
    """
    env = os.environ if environ is None else environ
    config_path = Path(path or env.get(CONFIG_PATH_ENV) or DEFAULT_CONFIG_PATH)
    if not config_path.is_file():
        raise OrchestrationConfigError(f"No existe el archivo de configuración: {config_path}")
    try:
        raw: Any = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    except yaml.YAMLError as error:
        raise OrchestrationConfigError(f"{config_path}: YAML mal formado: {error}") from error

    section = raw.get("orchestration") if isinstance(raw, dict) else None
    if not isinstance(section, dict):
        raise OrchestrationConfigError(f"{config_path}: falta la sección 'orchestration'")

    start, end = _date(section, "start_date"), _date(section, "end_date")
    if start > end:
        raise OrchestrationConfigError(f"start_date ({start}) es posterior a end_date ({end})")
    retries = _int_at_least(section, "retries", 0)
    delay = _int_at_least(section, "retry_delay_minutes", 1)
    alerts_enabled = _alerts_enabled(section, env)

    alert_email = None
    if alerts_enabled:
        missing = [name for name in (ALERT_EMAIL_ENV, *SMTP_ENVS) if not env.get(name)]
        if missing:
            raise OrchestrationConfigError(
                f"Alertas activas, pero faltan variables de entorno: {', '.join(missing)}"
            )
        alert_email = env[ALERT_EMAIL_ENV]

    return OrchestrationConfig(
        start_date=start,
        end_date=end,
        retries=retries,
        retry_delay=timedelta(minutes=delay),
        alerts_enabled=alerts_enabled,
        alert_email=alert_email,
    )
