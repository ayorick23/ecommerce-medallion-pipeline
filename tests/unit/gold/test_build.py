import json
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path

import duckdb
import pytest

from medallion.common.config import PipelineConfig
from medallion.common.storage import STAGING_DIR, StorageError
from medallion.gold import build
from medallion.gold.build import (
    GoldBuildError,
    check_not_backwards,
    gold_build,
    read_gold_build,
)
from medallion.gold.dbt import GOLD_DB_ENV, warehouse_path

D = date(2017, 3, 15)
BUILT_AT = datetime(2026, 9, 28, 12, 0, tzinfo=UTC)
SILVER_BUILT_AT = "2026-09-28T11:00:00+00:00"


def _config(root: Path) -> PipelineConfig:
    return PipelineConfig(storage_root=str(root), sources={}, early_arriving_grace_days=120)


def _write_manifest(root: Path, as_of: date) -> None:
    silver = root / "silver"
    silver.mkdir(parents=True, exist_ok=True)
    manifest = {"as_of": as_of.isoformat(), "built_at": SILVER_BUILT_AT}
    (silver / "_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")


class FakeDbt:
    """Hace de dbt: agrega una fila a ``fct_prueba`` en el archivo que recibe."""

    def __init__(self, succeed: bool = True) -> None:
        self.succeed = succeed
        self.calls: list[list[str]] = []
        self.rows_seen: list[int] = []

    def __call__(self, args: Sequence[str], env: Mapping[str, str]) -> bool:
        self.calls.append(list(args))
        db = Path(env[GOLD_DB_ENV])
        with duckdb.connect(str(db)) as con:
            con.execute("create table if not exists fct_prueba (x integer)")
            self.rows_seen.append(con.execute("select count(*) from fct_prueba").fetchone()[0])  # type: ignore[index]
            con.execute("insert into fct_prueba values (1)")
            # Como dbt: la vista nombra el catálogo, que DuckDB toma del nombre del archivo.
            con.execute("create schema if not exists intermediate")
            con.execute(
                "create or replace view intermediate.int_prueba as "
                f'select * from "{db.stem}".main.fct_prueba'
            )
        return self.succeed


@pytest.fixture
def root(tmp_path: Path) -> Path:
    _write_manifest(tmp_path, D)
    return tmp_path


@pytest.fixture
def fake_dbt(monkeypatch: pytest.MonkeyPatch) -> FakeDbt:
    fake = FakeDbt()
    monkeypatch.setattr(build, "run_dbt", fake)
    return fake


def _staging_leftovers(root: Path) -> list[Path]:
    staging = root / "gold" / STAGING_DIR
    return list(staging.rglob("*")) if staging.exists() else []


def test_publishes_gold_with_build_info(root: Path, fake_dbt: FakeDbt) -> None:
    info = gold_build(D, _config(root), BUILT_AT)

    assert fake_dbt.calls == [["build"]]
    assert info["filas"] == {"fct_prueba": 1}
    meta = read_gold_build(warehouse_path(_config(root)))
    assert meta is not None
    assert meta["as_of"] == D
    assert meta["built_at"] == datetime(2026, 9, 28, 12, 0)
    assert meta["silver_built_at"] == datetime(2026, 9, 28, 11, 0)
    assert meta["full_refresh"] is False
    assert meta["filas"] == {"fct_prueba": 1}
    assert _staging_leftovers(root) == []


def test_views_still_work_in_published_gold(root: Path, fake_dbt: FakeDbt) -> None:
    # Regresión: con un temporal de otro nombre, la vista apuntaba a un catálogo
    # inexistente en el archivo publicado.
    gold_build(D, _config(root), BUILT_AT)

    with duckdb.connect(str(warehouse_path(_config(root))), read_only=True) as con:
        assert con.execute("select count(*) from intermediate.int_prueba").fetchone() == (1,)


def test_leftovers_of_an_interrupted_run_are_cleaned(root: Path, fake_dbt: FakeDbt) -> None:
    stale = root / "gold" / STAGING_DIR / "build" / "warehouse.duckdb"
    stale.parent.mkdir(parents=True)
    stale.write_bytes(b"no es un duckdb")

    gold_build(D, _config(root), BUILT_AT)

    assert _staging_leftovers(root) == []


def test_incremental_run_starts_from_previous_gold(root: Path, fake_dbt: FakeDbt) -> None:
    gold_build(D, _config(root), BUILT_AT)
    _write_manifest(root, date(2017, 3, 16))

    gold_build(date(2017, 3, 16), _config(root), BUILT_AT)

    assert fake_dbt.rows_seen == [0, 1]  # la segunda corrida vio la tabla de la primera


def test_full_refresh_starts_empty(root: Path, fake_dbt: FakeDbt) -> None:
    gold_build(D, _config(root), BUILT_AT)

    info = gold_build(D, _config(root), BUILT_AT, full_refresh=True)

    assert fake_dbt.calls[-1] == ["build", "--full-refresh"]
    assert fake_dbt.rows_seen == [0, 0]
    assert info["full_refresh"] is True


def test_rerunning_same_day_is_allowed(root: Path, fake_dbt: FakeDbt) -> None:
    gold_build(D, _config(root), BUILT_AT)
    gold_build(D, _config(root), BUILT_AT)

    assert len(fake_dbt.calls) == 2


def test_dbt_failure_leaves_previous_gold_intact(
    root: Path, fake_dbt: FakeDbt, monkeypatch: pytest.MonkeyPatch
) -> None:
    gold_build(D, _config(root), BUILT_AT)
    warehouse = warehouse_path(_config(root))
    before = warehouse.read_bytes()
    monkeypatch.setattr(build, "run_dbt", FakeDbt(succeed=False))

    with pytest.raises(GoldBuildError, match="dbt build falló"):
        gold_build(D, _config(root), BUILT_AT)

    assert warehouse.read_bytes() == before
    assert _staging_leftovers(root) == []


def test_first_run_failure_leaves_no_gold(root: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(build, "run_dbt", FakeDbt(succeed=False))

    with pytest.raises(GoldBuildError):
        gold_build(D, _config(root), BUILT_AT)

    assert not warehouse_path(_config(root)).exists()
    assert _staging_leftovers(root) == []


def test_missing_silver_manifest_fails_before_dbt(tmp_path: Path, fake_dbt: FakeDbt) -> None:
    with pytest.raises(GoldBuildError, match="Silver está incompleto"):
        gold_build(D, _config(tmp_path), BUILT_AT)

    assert fake_dbt.calls == []
    assert not (tmp_path / "gold").exists()


def test_silver_of_another_day_fails_before_dbt(root: Path, fake_dbt: FakeDbt) -> None:
    with pytest.raises(GoldBuildError, match="Silver es del 2017-03-15"):
        gold_build(date(2017, 3, 16), _config(root), BUILT_AT)

    assert fake_dbt.calls == []


def test_going_backwards_requires_full_refresh(root: Path, fake_dbt: FakeDbt) -> None:
    gold_build(D, _config(root), BUILT_AT)
    warehouse = warehouse_path(_config(root))
    before = warehouse.read_bytes()
    _write_manifest(root, date(2017, 3, 14))

    with pytest.raises(GoldBuildError, match="hace falta --full-refresh"):
        gold_build(date(2017, 3, 14), _config(root), BUILT_AT)

    assert warehouse.read_bytes() == before
    gold_build(date(2017, 3, 14), _config(root), BUILT_AT, full_refresh=True)


def test_remote_root_fails_before_anything(fake_dbt: FakeDbt) -> None:
    config = PipelineConfig(
        storage_root="abfs://lake@cuenta.dfs.core.windows.net",
        sources={},
        early_arriving_grace_days=120,
    )
    with pytest.raises(StorageError):
        gold_build(D, config, BUILT_AT)

    assert fake_dbt.calls == []


def test_built_at_needs_timezone(root: Path, fake_dbt: FakeDbt) -> None:
    with pytest.raises(ValueError, match="zona horaria"):
        gold_build(D, _config(root), datetime(2026, 9, 28))


def test_gold_without_build_info_has_no_guard(tmp_path: Path) -> None:
    # Un Gold escrito con gold-dbt (sin _gold_build) no bloquea a gold-build.
    warehouse = tmp_path / "warehouse.duckdb"
    with duckdb.connect(str(warehouse)) as con:
        con.execute("create table fct_prueba (x integer)")

    assert read_gold_build(warehouse) is None
    check_not_backwards(None, D, full_refresh=False)


def test_main_reports_guard_errors(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    config = tmp_path / "pipeline.yaml"
    config.write_text(
        f"storage:\n  root: {tmp_path.as_posix()}\nsources:\n  orders: a.csv\n"
        "silver:\n  early_arriving_grace_days: 120\n",
        encoding="utf-8",
    )

    assert build.main(["--dia", "2017-03-15", "--config", str(config)]) == 1
    assert "Silver está incompleto" in capsys.readouterr().err
