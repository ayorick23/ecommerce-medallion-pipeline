from datetime import date
from pathlib import Path

import polars as pl
import pytest

from bronze.storage import (
    STAGING_DIR,
    StorageError,
    partition_path,
    read_source,
    snapshot_batch_hash,
    snapshot_path,
    write_partition,
    write_snapshot,
)

DIA = date(2017, 3, 15)


def _batch(order_ids: list[str], dia: date = DIA) -> pl.DataFrame:
    return pl.DataFrame({"order_id": order_ids}).with_columns(
        pl.lit(dia, dtype=pl.Date).alias("dia_simulado"),
        pl.lit("hash-" + "-".join(order_ids), dtype=pl.String).alias("batch_hash"),
    )


def _read_table(bronze: Path, table: str) -> pl.DataFrame:
    return pl.read_parquet(bronze / table, hive_partitioning=True).sort("order_id")


def _staging_files(bronze: Path) -> list[Path]:
    staging = bronze / STAGING_DIR
    return list(staging.iterdir()) if staging.exists() else []


def test_read_source_keeps_every_column_as_text(tmp_path: Path) -> None:
    csv = tmp_path / "orders.csv"
    csv.write_bytes(b'order_id,price,comment\nA1,10.50,"linea\ncon, coma"\nA2,,\n')

    df = read_source(str(csv))

    assert df.schema == pl.Schema(dict.fromkeys(["order_id", "price", "comment"], pl.String))
    assert df.rows() == [("A1", "10.50", "linea\ncon, coma"), ("A2", None, None)]


def test_partition_goes_to_its_hive_path_without_the_partition_column(tmp_path: Path) -> None:
    write_partition(_batch(["a", "b"]), str(tmp_path), "orders", DIA)

    path = partition_path(str(tmp_path), "orders", DIA)
    assert path == tmp_path / "orders" / "dia_simulado=2017-03-15" / "part-0.parquet"
    assert "dia_simulado" not in pl.read_parquet(path).columns


def test_partition_column_is_rebuilt_as_date_from_the_path(tmp_path: Path) -> None:
    other = date(2017, 3, 16)
    write_partition(_batch(["a"]), str(tmp_path), "orders", DIA)
    write_partition(_batch(["b"], other), str(tmp_path), "orders", other)

    table = _read_table(tmp_path, "orders")

    assert table.schema["dia_simulado"] == pl.Date
    assert table.select("order_id", "dia_simulado").rows() == [("a", DIA), ("b", other)]


def test_rewriting_a_partition_replaces_it(tmp_path: Path) -> None:
    write_partition(_batch(["a", "b"]), str(tmp_path), "orders", DIA)
    write_partition(_batch(["c"]), str(tmp_path), "orders", DIA)

    assert _read_table(tmp_path, "orders")["order_id"].to_list() == ["c"]
    assert _staging_files(tmp_path) == []


def test_empty_batch_removes_an_existing_partition(tmp_path: Path) -> None:
    write_partition(_batch(["a"]), str(tmp_path), "orders", DIA)

    write_partition(_batch([]), str(tmp_path), "orders", DIA)

    assert not partition_path(str(tmp_path), "orders", DIA).parent.exists()


def test_empty_batch_without_previous_partition_writes_nothing(tmp_path: Path) -> None:
    write_partition(_batch([]), str(tmp_path), "orders", DIA)

    assert not (tmp_path / "orders").exists()


def test_rows_from_another_day_are_rejected(tmp_path: Path) -> None:
    with pytest.raises(StorageError, match="dia_simulado distinto"):
        write_partition(_batch(["a"], date(2017, 3, 16)), str(tmp_path), "orders", DIA)


def test_batch_without_partition_column_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(StorageError, match="dia_simulado"):
        write_partition(_batch(["a"]).drop("dia_simulado"), str(tmp_path), "orders", DIA)


def test_failed_write_keeps_previous_partition_and_leaves_no_temp_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    write_partition(_batch(["a"]), str(tmp_path), "orders", DIA)

    def broken_write(self: pl.DataFrame, file: Path) -> None:
        Path(file).write_bytes(b"a medio escribir")
        raise OSError("disco lleno")

    monkeypatch.setattr(pl.DataFrame, "write_parquet", broken_write)
    with pytest.raises(OSError, match="disco lleno"):
        write_partition(_batch(["b"]), str(tmp_path), "orders", DIA)

    assert _read_table(tmp_path, "orders")["order_id"].to_list() == ["a"]
    assert _staging_files(tmp_path) == []


def test_snapshot_keeps_dia_simulado_and_exposes_its_batch_hash(tmp_path: Path) -> None:
    assert snapshot_batch_hash(str(tmp_path), "customers") is None

    write_snapshot(_batch(["a", "b"]), str(tmp_path), "customers")

    stored = pl.read_parquet(snapshot_path(str(tmp_path), "customers"))
    assert stored["dia_simulado"].to_list() == [DIA, DIA]
    assert snapshot_batch_hash(str(tmp_path), "customers") == "hash-a-b"


def test_rewriting_a_snapshot_replaces_it(tmp_path: Path) -> None:
    write_snapshot(_batch(["a", "b"]), str(tmp_path), "customers")
    write_snapshot(_batch(["c"]), str(tmp_path), "customers")

    stored = pl.read_parquet(snapshot_path(str(tmp_path), "customers"))
    assert stored["order_id"].to_list() == ["c"]
    assert snapshot_batch_hash(str(tmp_path), "customers") == "hash-c"
    assert _staging_files(tmp_path) == []


@pytest.mark.parametrize("root", ["abfs://lake@cuenta.dfs.core.windows.net/bronze", "s3://b/x"])
def test_remote_storage_is_rejected(root: str) -> None:
    with pytest.raises(StorageError, match="Solo se admite almacenamiento local"):
        write_partition(_batch(["a"]), root, "orders", DIA)
