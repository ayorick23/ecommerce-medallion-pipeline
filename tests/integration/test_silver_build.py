"""``silver_build`` de punta a punta: Bronze en disco -> Silver escrito con manifiesto."""

from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import pytest

from medallion.common.storage import STAGING_DIR
from medallion.silver.build import (
    main,
    manifest_path,
    pending_path,
    read_manifest,
    silver_build,
    table_path,
)
from medallion.silver.schemas import SCHEMAS
from medallion.silver.validation import SilverValidationError
from tests.integration.conftest import (
    BRONZE_DAYS_WITH_DATA,
    FIRST_DAY,
    LAST_DAY,
    SILVER_CSV,
    SilverEnv,
    write_sources,
)

BUILT_AT = datetime(2026, 9, 25, 15, 4, 11, tzinfo=UTC)


def _silver_files(silver: Path) -> dict[Path, bytes]:
    return {p: p.read_bytes() for p in silver.rglob("*") if p.is_file()}


def test_build_writes_every_table_the_pending_file_and_the_manifest(silver_env: SilverEnv) -> None:
    silver_uri = str(silver_env.silver)

    manifest = silver_build(LAST_DAY, silver_env.config, BUILT_AT)

    for table in SCHEMAS:
        assert table_path(silver_uri, table).is_file(), table
    assert pending_path(silver_uri).is_file()
    assert list((silver_env.silver / STAGING_DIR).iterdir()) == []
    assert read_manifest(silver_uri) == manifest
    assert manifest == {
        "as_of": "2017-03-19",
        "built_at": "2026-09-25T15:04:11+00:00",
        "bronze_days": {
            "first": str(FIRST_DAY),
            "last": str(LAST_DAY),
            "count": BRONZE_DAYS_WITH_DATA,
        },
        "row_counts": {
            "orders": 3,
            "order_status_history": 10,
            "order_items": 3,
            "order_payments": 2,
            "order_reviews": 2,
            "customers": 3,
            "products": 2,
            "sellers": 1,
            "geolocation_agg": 2,
            "category_translation": 1,
        },
        "pending_reviews": 0,
    }


def test_written_tables_match_their_contract(silver_env: SilverEnv) -> None:
    silver_build(LAST_DAY, silver_env.config, BUILT_AT)

    for table, schema in SCHEMAS.items():
        schema.validate(pl.read_parquet(table_path(str(silver_env.silver), table)))


def test_there_is_no_manifest_before_the_first_build(silver_env: SilverEnv) -> None:
    assert read_manifest(str(silver_env.silver)) is None


def test_a_failed_validation_writes_nothing_and_keeps_the_previous_silver(
    silver_env: SilverEnv,
) -> None:
    silver_build(LAST_DAY, silver_env.config, BUILT_AT)
    before = _silver_files(silver_env.silver)

    items = SILVER_CSV["order_items"].replace(
        "o2,1,p1,s1,2017-03-21 00:00:00,99.90", "o2,1,p1,s1,2017-03-21 00:00:00,99.905"
    )
    write_sources(silver_env.root, {"order_items": items})
    silver_env.ingest()

    with pytest.raises(SilverValidationError) as error:
        silver_build(LAST_DAY, silver_env.config, datetime(2026, 9, 26, tzinfo=UTC))

    assert [(f.table, f.rule, f.columns) for f in error.value.failures] == [
        ("order_items", "tipo_no_parseable", ("price",)),
        ("order_items", "nulo_no_permitido", ("price",)),
    ]
    assert _silver_files(silver_env.silver) == before


def test_an_interrupted_write_leaves_silver_without_manifest(
    silver_env: SilverEnv, monkeypatch: pytest.MonkeyPatch
) -> None:
    silver_build(LAST_DAY, silver_env.config, BUILT_AT)

    def disk_full(_self: pl.DataFrame, _file: Path) -> None:
        raise OSError("disco lleno")

    monkeypatch.setattr(pl.DataFrame, "write_parquet", disk_full)
    with pytest.raises(OSError, match="disco lleno"):
        silver_build(LAST_DAY, silver_env.config, BUILT_AT)

    assert not manifest_path(str(silver_env.silver)).exists()
    assert read_manifest(str(silver_env.silver)) is None


def test_command_line_builds_silver(
    silver_env: SilverEnv, capsys: pytest.CaptureFixture[str]
) -> None:
    assert main(["--dia", str(LAST_DAY), "--config", str(silver_env.config_file)]) == 0

    output = capsys.readouterr().out
    assert "order_status_history          10" in output
    assert "Silver(2017-03-19) escrito" in output
    assert read_manifest(str(silver_env.silver)) is not None


def test_command_line_reports_validation_failures_and_exits_with_1(
    silver_env: SilverEnv, capsys: pytest.CaptureFixture[str]
) -> None:
    payments = SILVER_CSV["order_payments"].replace("o2,1,boleto,1", "o2,1,boleto,-1")
    write_sources(silver_env.root, {"order_payments": payments})
    silver_env.ingest()

    assert main(["--dia", str(LAST_DAY), "--config", str(silver_env.config_file)]) == 1

    error = capsys.readouterr().err
    assert "Silver no pasó la validación: 1 fallas; no se escribió nada." in error
    assert "order_payments [payment_installments] medida_imposible: 1 filas" in error
    assert not silver_env.silver.exists()
