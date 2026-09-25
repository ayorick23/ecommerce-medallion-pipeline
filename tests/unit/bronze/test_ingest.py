from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl
import pytest

from medallion.bronze.hashing import batch_hash
from medallion.bronze.ingest import TableResult, ingest_day
from medallion.bronze.storage import partition_path, snapshot_path
from medallion.bronze.tables import EVENT_TABLES, REFERENCE_TABLES
from medallion.common.config import PipelineConfig

D15, D16, D17 = date(2017, 3, 15), date(2017, 3, 16), date(2017, 3, 17)
INGESTED_AT = datetime(2026, 9, 24, 12, 0, tzinfo=UTC)


def _text(data: dict[str, list[str | None]]) -> pl.DataFrame:
    return pl.DataFrame(data, schema=dict.fromkeys(data, pl.String))


def _sources() -> dict[str, pl.DataFrame]:
    """o1: compra y aprobación el 15, carrier el 16, entrega el 17. o2: solo compra el 16."""
    return {
        "orders": _text(
            {
                "order_id": ["o1", "o2"],
                "order_purchase_timestamp": ["2017-03-15 10:00:00", "2017-03-16 09:00:00"],
                "order_approved_at": ["2017-03-15 11:00:00", None],
                "order_delivered_carrier_date": ["2017-03-16 08:00:00", None],
                "order_delivered_customer_date": ["2017-03-17 18:00:00", None],
            }
        ),
        "order_items": _text({"order_id": ["o1", "o1", "o2"], "order_item_id": ["1", "2", "1"]}),
        "order_payments": _text({"order_id": ["o1", "o2"], "payment_sequential": ["1", "1"]}),
        "order_reviews": _text(
            {
                "review_id": ["r1"],
                "order_id": ["o1"],
                "review_creation_date": ["2017-03-17 00:00:00"],
            }
        ),
        "customers": _text({"customer_id": ["c1"]}),
        "products": _text({"product_id": ["p1"]}),
        "sellers": _text({"seller_id": ["s1"]}),
        "geolocation": _text({"geolocation_zip_code_prefix": ["01037"]}),
        "category_translation": _text({"product_category_name": ["beleza_saude"]}),
    }


def _config(root: Path) -> PipelineConfig:
    return PipelineConfig(
        storage_root=str(root),
        sources={t: f"{t}.csv" for t in (*EVENT_TABLES, *REFERENCE_TABLES)},
        early_arriving_grace_days=120,
    )


def _actions(results: list[TableResult]) -> dict[str, tuple[str, int]]:
    return {r.table: (r.action, r.rows) for r in results}


def _partition(root: Path, table: str, dia: date) -> pl.DataFrame:
    return pl.read_parquet(partition_path(str(root / "bronze"), table, dia))


def _snapshot(root: Path, table: str) -> pl.DataFrame:
    return pl.read_parquet(snapshot_path(str(root / "bronze"), table))


def test_routes_each_event_table_to_its_days(tmp_path: Path) -> None:
    sources, config = _sources(), _config(tmp_path)

    by_day = {
        dia: _actions(ingest_day(dia, sources, config, INGESTED_AT)) for dia in (D15, D16, D17)
    }

    assert by_day[D15]["orders"] == ("written", 1)  # compra + aprobación: una sola fila
    assert by_day[D16]["orders"] == ("written", 2)  # o1 (carrier) re-emitido + o2 (compra)
    assert by_day[D17]["orders"] == ("written", 1)
    assert [by_day[d]["order_items"] for d in (D15, D16, D17)] == [
        ("written", 2),
        ("written", 1),
        ("empty", 0),
    ]
    assert [by_day[d]["order_reviews"][0] for d in (D15, D16, D17)] == ["empty", "empty", "written"]


def test_event_partition_keeps_source_values_and_lineage(tmp_path: Path) -> None:
    sources = _sources()

    ingest_day(D16, sources, _config(tmp_path), INGESTED_AT)

    stored = _partition(tmp_path, "order_items", D16)
    source_part = stored.select(sources["order_items"].columns)
    assert source_part.rows() == [("o2", "1")]
    assert stored["source_file"].to_list() == ["order_items.csv"]
    assert stored["batch_hash"].to_list() == [batch_hash(source_part)]
    assert stored["ingested_at"].to_list() == [INGESTED_AT]


def test_reference_snapshots_are_written_once_while_the_source_does_not_change(
    tmp_path: Path,
) -> None:
    sources, config = _sources(), _config(tmp_path)

    first = _actions(ingest_day(D15, sources, config, INGESTED_AT))
    second = _actions(ingest_day(D16, sources, config, INGESTED_AT))

    assert all(first[t] == ("written", 1) for t in REFERENCE_TABLES)
    assert all(second[t] == ("unchanged", 1) for t in REFERENCE_TABLES)
    assert _snapshot(tmp_path, "customers")["dia_simulado"].to_list() == [D15]


def test_changed_reference_source_rewrites_its_snapshot(tmp_path: Path) -> None:
    sources, config = _sources(), _config(tmp_path)
    ingest_day(D15, sources, config, INGESTED_AT)

    sources["customers"] = _text({"customer_id": ["c1", "c2"]})
    results = _actions(ingest_day(D16, sources, config, INGESTED_AT))

    assert results["customers"] == ("written", 2)
    assert results["products"] == ("unchanged", 1)
    assert _snapshot(tmp_path, "customers")["dia_simulado"].to_list() == [D16, D16]


def test_reprocessing_a_day_gives_the_same_content_except_ingested_at(tmp_path: Path) -> None:
    sources, config = _sources(), _config(tmp_path)
    ingest_day(D16, sources, config, INGESTED_AT)
    before = _partition(tmp_path, "orders", D16)

    later = datetime(2026, 9, 25, 8, 0, tzinfo=UTC)
    ingest_day(D16, sources, config, later)
    after = _partition(tmp_path, "orders", D16)

    assert after.drop("ingested_at").equals(before.drop("ingested_at"))
    assert after["ingested_at"].unique().to_list() == [later]


def test_day_without_source_rows_writes_no_partitions(tmp_path: Path) -> None:
    results = _actions(ingest_day(date(2017, 1, 1), _sources(), _config(tmp_path), INGESTED_AT))

    assert all(results[t] == ("empty", 0) for t in EVENT_TABLES)
    assert all(not (tmp_path / "bronze" / t).exists() for t in EVENT_TABLES)


def test_naive_ingested_at_fails_before_writing_anything(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="zona horaria"):
        ingest_day(D15, _sources(), _config(tmp_path), datetime(2026, 9, 24, 12, 0))

    assert not (tmp_path / "bronze").exists()
