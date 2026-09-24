"""`batch_hash`: SHA-256 de la serialización canónica del batch (ADR 0013, ADR 0014).

Cada valor se codifica con prefijo de longitud: nulo -> ``N``; no nulo ->
``S<n>:<valor>``, con ``<n>`` en bytes UTF-8. Un registro es la concatenación
de sus valores terminada en ``\\n``. Se hashea un encabezado con los nombres de
columna seguido de las filas ordenadas por su codificación (orden de bytes).
"""

import hashlib

import polars as pl

from medallion.bronze.lineage import LINEAGE_COLUMNS


def _encode_value(column: str) -> pl.Expr:
    value = pl.col(column)
    return (
        pl.when(value.is_null())
        .then(pl.lit("N"))
        .otherwise(
            pl.concat_str(pl.lit("S"), value.str.len_bytes().cast(pl.String), pl.lit(":"), value)
        )
    )


def _encode_records(df: pl.DataFrame) -> pl.Series:
    return df.select(
        pl.concat_str(*(_encode_value(c) for c in df.columns), pl.lit("\n")).alias("record")
    ).to_series()


def batch_hash(df: pl.DataFrame) -> str:
    """SHA-256 (hex) del contenido del batch, independiente del orden de sus filas.

    Recibe solo las columnas fuente, todas texto (ADR 0011), en el orden de la
    fuente: los nombres y el orden de columnas forman parte del hash.
    """
    lineage = set(LINEAGE_COLUMNS) & set(df.columns)
    if lineage:
        raise ValueError(f"El hash se calcula sin columnas de linaje: {sorted(lineage)}")
    non_text = [c for c, dtype in df.schema.items() if dtype != pl.String]
    if non_text:
        raise ValueError(f"Las columnas fuente de Bronze deben ser texto: {non_text}")

    header = _encode_records(pl.DataFrame({c: [c] for c in df.columns}))
    rows = _encode_records(df).sort()
    canonical = pl.concat([header, rows]).str.join("").item()
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
