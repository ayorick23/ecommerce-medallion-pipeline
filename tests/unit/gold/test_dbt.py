import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from medallion.common.config import PipelineConfig
from medallion.common.storage import StorageError
from medallion.gold import dbt
from medallion.gold.dbt import (
    DBT_ENTRY_POINT,
    DBT_PROJECT_DIR,
    GOLD_DB_ENV,
    SILVER_URI_ENV,
    dbt_env,
    run_dbt,
    warehouse_path,
)


def _config(root: str) -> PipelineConfig:
    return PipelineConfig(storage_root=root, sources={}, early_arriving_grace_days=120)


def test_env_has_absolute_posix_paths(tmp_path: Path) -> None:
    config = _config(str(tmp_path / "data"))
    env = dbt_env(config, warehouse_path(config))

    assert env[SILVER_URI_ENV] == (tmp_path / "data" / "silver").resolve().as_posix()
    assert env[GOLD_DB_ENV] == (tmp_path / "data" / "gold" / "warehouse.duckdb").as_posix()
    assert all(Path(value).is_absolute() and "\\" not in value for value in env.values())


def test_relative_root_resolves_against_cwd(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)

    env = dbt_env(_config("./data"), warehouse_path(_config("./data")))

    assert env[SILVER_URI_ENV] == (tmp_path / "data" / "silver").resolve().as_posix()


def test_remote_root_is_an_error() -> None:
    with pytest.raises(StorageError, match="Solo se admite almacenamiento local"):
        warehouse_path(_config("abfs://lake@cuenta.dfs.core.windows.net"))


class _FakeRun:
    """Reemplaza ``subprocess.run``: registra el comando y el entorno del hijo."""

    def __init__(self) -> None:
        self.commands: list[list[str]] = []
        self.envs: list[dict[str, str]] = []

    def __call__(self, command: list[str], env: dict[str, str], check: bool) -> Any:
        self.commands.append(command)
        self.envs.append(env)
        return subprocess.CompletedProcess(command, 1 if "falla" in command else 0)


@pytest.fixture
def fake_run(monkeypatch: pytest.MonkeyPatch) -> _FakeRun:
    fake = _FakeRun()
    monkeypatch.setattr(subprocess, "run", fake)
    monkeypatch.delenv(SILVER_URI_ENV, raising=False)
    monkeypatch.delenv(GOLD_DB_ENV, raising=False)
    return fake


def test_run_dbt_runs_dbt_in_a_child_process(fake_run: _FakeRun) -> None:
    env = {SILVER_URI_ENV: "/s", GOLD_DB_ENV: "/g.duckdb"}

    assert run_dbt(["build", "-s", "dim_cliente"], env) is True

    project = str(DBT_PROJECT_DIR.resolve())
    assert fake_run.commands == [
        [sys.executable, "-c", DBT_ENTRY_POINT, "build", "-s", "dim_cliente"]
        + ["--project-dir", project, "--profiles-dir", project]
    ]
    child_env = fake_run.envs[0]
    assert {k: child_env[k] for k in env} == env
    assert child_env["PATH"] == os.environ["PATH"]  # hereda el resto del entorno


def test_run_dbt_overrides_existing_env_only_in_the_child(
    fake_run: _FakeRun, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(SILVER_URI_ENV, "/puesta/a/mano")

    run_dbt(["run"], {SILVER_URI_ENV: "/de/la/config", GOLD_DB_ENV: "/g.duckdb"})

    assert fake_run.envs[0][SILVER_URI_ENV] == "/de/la/config"
    assert os.environ[SILVER_URI_ENV] == "/puesta/a/mano"
    assert GOLD_DB_ENV not in os.environ


def test_run_dbt_reports_failure(fake_run: _FakeRun) -> None:
    assert run_dbt(["falla"], {SILVER_URI_ENV: "/s", GOLD_DB_ENV: "/g"}) is False


@pytest.mark.parametrize("argv", [[], ["--version"]])
def test_main_requires_a_dbt_command(argv: list[str], capsys: pytest.CaptureFixture[str]) -> None:
    assert dbt.main(argv) == 2
    assert "uso: gold-dbt" in capsys.readouterr().err
