"""Columnas de linaje de Bronze (docs/schemas.md, ADR 0011).

Son las únicas columnas tipadas de Bronze: las genera el pipeline, no la fuente.
"""

from datetime import UTC, date, datetime
from typing import Final

import polars as pl

LINEAGE_COLUMNS: Final = ("dia_simulado", "ingested_at", "source_file", "batch_hash")


def add_lineage(
    df: pl.DataFrame,
    *,
    dia: date,
    ingested_at: datetime,
    source_file: str,
    batch_hash: str,
) -> pl.DataFrame:
    """Agrega las columnas de linaje, constantes en todo el batch, después de las fuente.

    ``ingested_at`` debe traer zona horaria; se guarda normalizada a UTC para
    que el linaje no dependa de la zona de la máquina que corrió la ingesta.
    """
    if ingested_at.tzinfo is None:
        raise ValueError("ingested_at debe tener zona horaria (p. ej. datetime.now(UTC))")
    collisions = set(LINEAGE_COLUMNS) & set(df.columns)
    if collisions:
        raise ValueError(f"El batch ya trae columnas de linaje: {sorted(collisions)}")

    return df.with_columns(
        pl.lit(dia, dtype=pl.Date).alias("dia_simulado"),
        pl.lit(ingested_at.astimezone(UTC), dtype=pl.Datetime("us", "UTC")).alias("ingested_at"),
        pl.lit(source_file, dtype=pl.String).alias("source_file"),
        pl.lit(batch_hash, dtype=pl.String).alias("batch_hash"),
    )
