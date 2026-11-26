import os
import subprocess
import sys
from collections.abc import Callable
from datetime import date
from pathlib import Path
from typing import Any

import pytest

from medallion import orchestration
from medallion.bronze.routing import RoutingError
from medallion.common.config import STORAGE_ROOT_ENV, ConfigError, PipelineConfig
from medallion.common.storage import StorageError
from medallion.gold.build import GoldBuildError
from medallion.orchestration import (
    EXIT_DETERMINISTIC,
    gold_needs_full_refresh,
    main,
    run_gold,
)
from medallion.silver.validation import Failure, SilverValidationError

D = date(2017, 3, 10)
CONFIG = PipelineConfig(storage_root="unused", sources={}, early_arriving_grace_days=120)


@pytest.fixture
def fake_config(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(orchestration, "load_config", lambda path: CONFIG)


def _layer_that(action: Callable[[], str]) -> Callable[[date, PipelineConfig], str]:
    def layer(dia: date, config: PipelineConfig) -> str:
        return action()

    return layer


def _raise(error: BaseException) -> Callable[[], str]:
    def action() -> str:
        raise error

    return action


@pytest.mark.parametrize(
    ("previous", "expected"),
    [
        (None, False),  # Gold todavía no existe
        ({"as_of": date(2017, 3, 9)}, False),  # el día siguiente: incremental
        ({"as_of": D}, True),  # el mismo día otra vez
        ({"as_of": date(2018, 10, 17)}, True),  # rebobinar (Clear + Future)
    ],
)
def test_gold_full_refresh_when_day_already_reached(
    previous: dict[str, Any] | None, expected: bool
) -> None:
    assert gold_needs_full_refresh(previous, D) is expected


@pytest.mark.parametrize(("as_of", "expected"), [(date(2017, 3, 9), False), (D, True)])
def test_run_gold_passes_full_refresh_decision(
    monkeypatch: pytest.MonkeyPatch, as_of: date, expected: bool
) -> None:
    calls: list[bool] = []
    monkeypatch.setattr(orchestration, "warehouse_path", lambda config: Path("unused"))
    monkeypatch.setattr(orchestration, "read_gold_build", lambda path: {"as_of": as_of})
    monkeypatch.setattr(
        orchestration,
        "gold_build",
        lambda dia, config, built_at, full_refresh: calls.append(full_refresh),
    )
    run_gold(D, CONFIG)
    assert calls == [expected]


@pytest.mark.usefixtures("fake_config")
def test_success_exits_zero(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[date] = []

    def layer(dia: date, config: PipelineConfig) -> str:
        seen.append(dia)
        return "ok"

    monkeypatch.setitem(orchestration.LAYERS, "silver", layer)
    assert main(["silver", D.isoformat()]) == 0
    assert seen == [D]


@pytest.mark.parametrize(
    "error",
    [
        ConfigError("config"),
        RoutingError("ruteo"),
        StorageError("almacenamiento"),
        SilverValidationError([Failure("orders", "pk_duplicada", ("order_id",), 2)]),
        GoldBuildError("gold"),
    ],
    ids=lambda e: type(e).__name__,
)
@pytest.mark.usefixtures("fake_config")
def test_known_errors_exit_deterministic(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], error: Exception
) -> None:
    monkeypatch.setitem(orchestration.LAYERS, "gold", _layer_that(_raise(error)))
    assert main(["gold", D.isoformat()]) == EXIT_DETERMINISTIC
    assert type(error).__name__ in capsys.readouterr().err


def test_every_deterministic_error_is_covered() -> None:
    # Si se agrega una excepción conocida, el test parametrizado de arriba tiene que cubrirla.
    assert set(orchestration.DETERMINISTIC_ERRORS) == {
        ConfigError,
        RoutingError,
        StorageError,
        SilverValidationError,
        GoldBuildError,
    }


@pytest.mark.usefixtures("fake_config")
def test_unexpected_error_escapes(monkeypatch: pytest.MonkeyPatch) -> None:
    # Escapa para que Python salga con 1 y Airflow reintente (p. ej. el .duckdb bloqueado).
    monkeypatch.setitem(orchestration.LAYERS, "bronze", _layer_that(_raise(OSError("lock"))))
    with pytest.raises(OSError, match="lock"):
        main(["bronze", D.isoformat()])


def test_missing_config_is_deterministic(tmp_path: Path) -> None:
    # load_config corre dentro del try: una configuración rota no se reintenta.
    assert main(["silver", D.isoformat(), "--config", str(tmp_path / "no.yaml")]) == 3


REPO_CONFIG = Path(__file__).resolve().parents[2] / "config" / "pipeline.yaml"


def _run_module(*args: str, storage_root: Path) -> int:
    env = {**os.environ, STORAGE_ROOT_ENV: str(storage_root)}
    command = [sys.executable, "-m", "medallion.orchestration", *args]
    return subprocess.run(command, env=env, capture_output=True, check=False).returncode


def test_module_exit_codes_as_seen_by_airflow(tmp_path: Path) -> None:
    # Lo que ve la task de Airflow: el código del proceso, no la excepción.
    missing_config = ["--config", str(tmp_path / "no.yaml")]
    assert _run_module("silver", D.isoformat(), *missing_config, storage_root=tmp_path) == 3
    # Raíz sin data/raw: el CSV fuente no existe y la excepción de Polars escapa.
    repo_config = ["--config", str(REPO_CONFIG)]
    assert _run_module("bronze", D.isoformat(), *repo_config, storage_root=tmp_path) == 1
