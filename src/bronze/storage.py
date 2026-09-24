"""Lectura de fuentes y escritura atómica de Bronze (ADR 0011, 0012, 0015).

Cada partición de eventos y cada snapshot es un solo archivo Parquet. Se
escribe primero en ``bronze/_staging/`` y se mueve a su destino con
``os.replace``, que reemplaza el archivo de forma atómica: un lector nunca ve
un archivo a medio escribir. Solo almacenamiento local en la Fase 2.
"""

import os
import uuid
from datetime import date
from pathlib import Path
from typing import Final

import polars as pl

PARTITION_COLUMN: Final = "dia_simulado"
PARTITION_FILE: Final = "part-0.parquet"
SNAPSHOT_FILE: Final = "snapshot.parquet"
STAGING_DIR: Final = "_staging"


class StorageError(ValueError):
    """La escritura o lectura no se puede hacer con las garantías de Bronze."""


def _local_path(uri: str) -> Path:
    if "://" in uri:
        raise StorageError(
            f"Solo se admite almacenamiento local en la Fase 2 (ADR 0015); recibido: {uri!r}"
        )
    return Path(uri)


def partition_path(bronze_uri: str, table: str, dia: date) -> Path:
    return (
        _local_path(bronze_uri) / table / f"{PARTITION_COLUMN}={dia.isoformat()}" / PARTITION_FILE
    )


def snapshot_path(bronze_uri: str, table: str) -> Path:
    return _local_path(bronze_uri) / table / SNAPSHOT_FILE


def read_source(uri: str) -> pl.DataFrame:
    """Lee un CSV fuente con todas las columnas como texto (ADR 0011)."""
    return pl.read_csv(_local_path(uri), infer_schema=False)


def _atomic_write(df: pl.DataFrame, target: Path, *, bronze_uri: str, table: str) -> None:
    staging = _local_path(bronze_uri) / STAGING_DIR
    staging.mkdir(parents=True, exist_ok=True)
    tmp = staging / f"{table}-{uuid.uuid4().hex}.parquet"
    try:
        df.write_parquet(tmp)
        target.parent.mkdir(parents=True, exist_ok=True)
        os.replace(tmp, target)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise


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
    _atomic_write(df.drop(PARTITION_COLUMN), target, bronze_uri=bronze_uri, table=table)


def write_snapshot(df: pl.DataFrame, bronze_uri: str, table: str) -> None:
    """Reemplaza el snapshot completo de una tabla de referencia."""
    _atomic_write(df, snapshot_path(bronze_uri, table), bronze_uri=bronze_uri, table=table)


def snapshot_batch_hash(bronze_uri: str, table: str) -> str | None:
    """``batch_hash`` del snapshot actual, o ``None`` si no existe o está vacío."""
    path = snapshot_path(bronze_uri, table)
    if not path.is_file():
        return None
    stored = pl.read_parquet(path, columns=["batch_hash"], n_rows=1)
    hashes: list[str] = stored["batch_hash"].to_list()
    return hashes[0] if hashes else None
