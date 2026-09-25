from datetime import date
from pathlib import Path

import polars as pl
import pytest

from medallion.bronze.storage import write_partition, write_snapshot
from medallion.bronze.tables import EVENT_TABLES, REFERENCE_TABLES
from medallion.common.storage import StorageError
from medallion.silver.read import read_bronze, read_event_table, read_snapshot

D15, D16, D17 = date(2017, 3, 15), date(2017, 3, 16), date(2017, 3, 17)


def _write_day(bronze: Path, table: str, dia: date, ids: list[str]) -> None:
    batch = pl.DataFrame({"order_id": ids}).with_columns(
        pl.lit(dia, dtype=pl.Date).alias("dia_simulado")
    )
    write_partition(batch, str(bronze), table, dia)


def test_reads_only_partitions_up_to_the_day(tmp_path: Path) -> None:
    for dia, ids in [(D15, ["a"]), (D16, ["b", "c"]), (D17, ["d"])]:
        _write_day(tmp_path, "orders", dia, ids)

    df = read_event_table(str(tmp_path), "orders", D16)

    assert df.schema["dia_simulado"] == pl.Date
    assert sorted(df.select("order_id", "dia_simulado").rows()) == [
        ("a", D15),
        ("b", D16),
        ("c", D16),
    ]


def test_a_day_before_the_first_partition_gives_an_empty_table(tmp_path: Path) -> None:
    _write_day(tmp_path, "orders", D16, ["a"])

    df = read_event_table(str(tmp_path), "orders", D15)

    assert df.height == 0
    assert df.columns == ["order_id", "dia_simulado"]


def test_a_table_never_ingested_is_an_error(tmp_path: Path) -> None:
    with pytest.raises(StorageError, match="no tiene particiones de 'orders'"):
        read_event_table(str(tmp_path), "orders", D15)


def test_a_missing_snapshot_is_an_error(tmp_path: Path) -> None:
    with pytest.raises(StorageError, match="no tiene el snapshot de 'customers'"):
        read_snapshot(str(tmp_path), "customers")


def test_read_bronze_returns_every_table(tmp_path: Path) -> None:
    for table in EVENT_TABLES:
        _write_day(tmp_path, table, D15, [f"{table}-15"])
        _write_day(tmp_path, table, D17, [f"{table}-17"])
    for table in REFERENCE_TABLES:
        write_snapshot(pl.DataFrame({"id": [table]}), str(tmp_path), table)

    tables = read_bronze(str(tmp_path), D16)

    assert set(tables) == {*EVENT_TABLES, *REFERENCE_TABLES}
    for table in EVENT_TABLES:
        assert tables[table]["order_id"].to_list() == [f"{table}-15"]
    for table in REFERENCE_TABLES:
        assert tables[table]["id"].to_list() == [table]
