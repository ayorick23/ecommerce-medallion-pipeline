import os
from pathlib import Path
from typing import Any

import pytest

from medallion.common.config import PipelineConfig
from medallion.common.storage import StorageError
from medallion.gold import dbt
from medallion.gold.dbt import (
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


class _FakeRunner:
    calls: list[list[str]] = []
    env_seen: dict[str, str | None] = {}

    def invoke(self, args: list[str]) -> Any:
        _FakeRunner.calls.append(args)
        _FakeRunner.env_seen = {k: os.environ.get(k) for k in (SILVER_URI_ENV, GOLD_DB_ENV)}
        return type("Result", (), {"success": args[0] != "falla"})()


@pytest.fixture
def fake_runner(monkeypatch: pytest.MonkeyPatch) -> type[_FakeRunner]:
    import dbt.cli.main

    _FakeRunner.calls = []
    monkeypatch.setattr(dbt.cli.main, "dbtRunner", _FakeRunner)
    monkeypatch.delenv(SILVER_URI_ENV, raising=False)
    monkeypatch.delenv(GOLD_DB_ENV, raising=False)
    return _FakeRunner


def test_run_dbt_sets_env_and_points_to_project(fake_runner: type[_FakeRunner]) -> None:
    env = {SILVER_URI_ENV: "/s", GOLD_DB_ENV: "/g.duckdb"}

    assert run_dbt(["build", "-s", "dim_cliente"], env) is True

    project = str(DBT_PROJECT_DIR.resolve())
    assert fake_runner.calls == [
        ["build", "-s", "dim_cliente", "--project-dir", project, "--profiles-dir", project]
    ]
    assert fake_runner.env_seen == env


def test_run_dbt_overrides_existing_env(
    fake_runner: type[_FakeRunner], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(SILVER_URI_ENV, "/puesta/a/mano")

    run_dbt(["run"], {SILVER_URI_ENV: "/de/la/config", GOLD_DB_ENV: "/g.duckdb"})

    assert fake_runner.env_seen[SILVER_URI_ENV] == "/de/la/config"


def test_run_dbt_reports_failure(fake_runner: type[_FakeRunner]) -> None:
    assert run_dbt(["falla"], {SILVER_URI_ENV: "/s", GOLD_DB_ENV: "/g"}) is False


@pytest.mark.parametrize("argv", [[], ["--version"]])
def test_main_requires_a_dbt_command(argv: list[str], capsys: pytest.CaptureFixture[str]) -> None:
    assert dbt.main(argv) == 2
    assert "uso: gold-dbt" in capsys.readouterr().err
