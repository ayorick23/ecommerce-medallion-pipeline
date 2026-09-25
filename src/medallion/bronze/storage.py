"""Lectura de fuentes y escritura de Bronze (ADR 0011, 0012, 0015).

Cada partición de eventos y cada snapshot es un solo archivo Parquet, escrito
de forma atómica con ``medallion.common.storage.atomic_write``.
"""

from datetime import date
from pathlib import Path
from typing import Final

import polars as pl

from medallion.common.storage import StorageError, atomic_write, local_path

PARTITION_COLUMN: Final = "dia_simulado"
PARTITION_FILE: Final = "part-0.parquet"
SNAPSHOT_FILE: Final = "snapshot.parquet"


def partition_path(bronze_uri: str, table: str, dia: date) -> Path:
    return local_path(bronze_uri) / table / f"{PARTITION_COLUMN}={dia.isoformat()}" / PARTITION_FILE


def snapshot_path(bronze_uri: str, table: str) -> Path:
    return local_path(bronze_uri) / table / SNAPSHOT_FILE


def read_source(uri: str) -> pl.DataFrame:
    """Lee un CSV fuente con todas las columnas como texto (ADR 0011)."""
    return pl.read_csv(local_path(uri), infer_schema=False)


def write_partition(df: pl.DataFrame, bronze_uri: str, table: str, dia: date) -> None:
    """Reemplaza la partición ``dia`` de una tabla de eventos.

    ``dia_simulado`` debe coincidir con ``dia`` en todas las filas y no se
    guarda en el archivo: lo da la ruta Hive. Un batch vacío borra la partición.
    """
    if PARTITION_COLUMN not in df.columns:
        raise StorageError(f"{table}: el batch no trae la columna {PARTITION_COLUMN!r}")
    other_days = df.filter(pl.col(PARTITION_COLUMN) != dia)
    if other_days.height > 0:
        raise StorageError(
            f"{table}: {other_days.height} filas con {PARTITION_COLUMN} distinto de {dia}"
        )

    target = partition_path(bronze_uri, table, dia)
    if df.height == 0:
        target.unlink(missing_ok=True)
        if target.parent.exists():
            target.parent.rmdir()
        return
    atomic_write(target, bronze_uri, df.drop(PARTITION_COLUMN).write_parquet)


def write_snapshot(df: pl.DataFrame, bronze_uri: str, table: str) -> None:
    """Reemplaza el snapshot completo de una tabla de referencia."""
    atomic_write(snapshot_path(bronze_uri, table), bronze_uri, df.write_parquet)


def snapshot_batch_hash(bronze_uri: str, table: str) -> str | None:
    """``batch_hash`` del snapshot actual, o ``None`` si no existe o está vacío."""
    path = snapshot_path(bronze_uri, table)
    if not path.is_file():
        return None
    stored = pl.read_parquet(path, columns=["batch_hash"], n_rows=1)
    hashes: list[str] = stored["batch_hash"].to_list()
    return hashes[0] if hashes else None
