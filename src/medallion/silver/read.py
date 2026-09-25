"""Lectura de Bronze "a la fecha D" (ADR 0017).

Silver(D) se construye con todas las particiones de Bronze con
``dia_simulado <= D`` más los snapshots de referencia. Una tabla ausente en
Bronze es un error: significa que Bronze no se ingirió, no que esté vacía.
"""

from datetime import date

import polars as pl

from medallion.bronze.storage import PARTITION_COLUMN, PARTITION_FILE, snapshot_path
from medallion.bronze.tables import EVENT_TABLES, REFERENCE_TABLES
from medallion.common.storage import StorageError, local_path


def read_event_table(bronze_uri: str, table: str, dia: date) -> pl.DataFrame:
    """Filas de las particiones de ``table`` con ``dia_simulado <= dia``.

    ``dia_simulado`` se reconstruye como ``Date`` a partir de la ruta Hive. El
    filtro por partición descarta archivos sin leerlos.
    """
    path = local_path(bronze_uri) / table
    if next(path.glob(f"{PARTITION_COLUMN}=*/{PARTITION_FILE}"), None) is None:
        raise StorageError(f"Bronze no tiene particiones de {table!r} en {path}")
    return (
        pl.scan_parquet(path, hive_partitioning=True, hive_schema={PARTITION_COLUMN: pl.Date})
        .filter(pl.col(PARTITION_COLUMN) <= dia)
        .collect()
    )


def read_snapshot(bronze_uri: str, table: str) -> pl.DataFrame:
    path = snapshot_path(bronze_uri, table)
    if not path.is_file():
        raise StorageError(f"Bronze no tiene el snapshot de {table!r} en {path}")
    return pl.read_parquet(path)


def read_bronze(bronze_uri: str, dia: date) -> dict[str, pl.DataFrame]:
    """Todas las tablas de Bronze tal como se conocían al final de ``dia``."""
    return {
        **{table: read_event_table(bronze_uri, table, dia) for table in EVENT_TABLES},
        **{table: read_snapshot(bronze_uri, table) for table in REFERENCE_TABLES},
    }
