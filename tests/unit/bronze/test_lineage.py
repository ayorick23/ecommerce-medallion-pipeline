from datetime import UTC, date, datetime, timedelta, timezone

import polars as pl
import pytest

from medallion.bronze.lineage import LINEAGE_COLUMNS, add_lineage

DIA = date(2017, 3, 15)
INGESTED_AT = datetime(2026, 9, 23, 14, 30, tzinfo=UTC)


def _batch() -> pl.DataFrame:
    return pl.DataFrame({"order_id": ["a", "b"], "order_status": ["delivered", None]})


def _lineage(df: pl.DataFrame, ingested_at: datetime = INGESTED_AT) -> pl.DataFrame:
    return add_lineage(
        df, dia=DIA, ingested_at=ingested_at, source_file="orders.csv", batch_hash="abc123"
    )


def test_appends_typed_lineage_columns_after_source_columns() -> None:
    result = _lineage(_batch())

    assert result.columns == ["order_id", "order_status", *LINEAGE_COLUMNS]
    assert result.schema["dia_simulado"] == pl.Date
    assert result.schema["ingested_at"] == pl.Datetime("us", "UTC")
    assert result.schema["source_file"] == pl.String
    assert result.schema["batch_hash"] == pl.String


def test_lineage_is_constant_for_the_whole_batch_and_source_is_untouched() -> None:
    batch = _batch()

    result = _lineage(batch)

    assert result.select(batch.columns).equals(batch)
    assert result.row(0)[2:] == result.row(1)[2:] == (DIA, INGESTED_AT, "orders.csv", "abc123")


def test_ingested_at_is_normalized_to_utc() -> None:
    local = datetime(2026, 9, 23, 8, 30, tzinfo=timezone(timedelta(hours=-6)))

    result = _lineage(_batch(), ingested_at=local)

    assert result["ingested_at"][0] == INGESTED_AT


def test_empty_batch_keeps_schema() -> None:
    result = _lineage(_batch().clear())

    assert result.height == 0
    assert result.columns == ["order_id", "order_status", *LINEAGE_COLUMNS]


def test_naive_ingested_at_is_rejected() -> None:
    with pytest.raises(ValueError, match="zona horaria"):
        _lineage(_batch(), ingested_at=datetime(2026, 9, 23, 14, 30))


def test_batch_that_already_has_lineage_columns_is_rejected() -> None:
    with pytest.raises(ValueError, match="batch_hash"):
        _lineage(_batch().with_columns(batch_hash=pl.lit("x")))
