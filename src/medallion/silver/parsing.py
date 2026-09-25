"""Conversión de las columnas de Bronze (texto, ADR 0011) a los tipos de Silver.

Una falla de conversión no corta la ejecución: se devuelve como ``Failure`` para
que el reporte de la corrida las reúna todas (ADR 0023), y el valor que no se
pudo convertir queda nulo. Por eso un valor inválido en una columna no nullable
aparece dos veces en el reporte: como ``tipo_no_parseable`` y como
``nulo_no_permitido``.

Cada clase de columna tiene un formato exacto que se verifica **antes** de
convertir: Polars trunca en silencio ``"12.345"`` a ``12.34`` al pasarlo a
``Decimal(18,2)`` (ADR 0022), y un código postal no debe perder sus ceros a la
izquierda (ADR 0021). El signo se admite en números para que un negativo se
reporte como medida imposible, no como formato inválido.
"""

from collections.abc import Mapping
from typing import Final

import polars as pl

from medallion.bronze.routing import TIMESTAMP_FORMAT
from medallion.silver.tables import DTYPES, TABLE_COLUMNS, TIMESTAMP_DTYPE, ColumnSpec, Kind
from medallion.silver.validation import Failure, examples

FORMATS: Final[dict[Kind, str]] = {
    "timestamp": r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$",
    "integer": r"^-?\d+$",
    "money": r"^-?\d+(\.\d{1,2})?$",
    "coordinate": r"^-?\d+(\.\d+)?$",
    "zip_code": r"^\d{5}$",
}


def _convert(raw: pl.Series, kind: Kind) -> pl.Series:
    if kind == "timestamp":
        return raw.str.strptime(TIMESTAMP_DTYPE, TIMESTAMP_FORMAT, strict=False)
    return raw.cast(DTYPES[kind], strict=False)


def _parse_column(
    df: pl.DataFrame, spec: ColumnSpec, table: str
) -> tuple[pl.Series, Failure | None]:
    if spec.source not in df.columns:
        missing = pl.Series(spec.name, [None] * df.height, dtype=DTYPES[spec.kind])
        return missing, Failure(table, "columna_faltante", (spec.name,), df.height)

    raw = df.get_column(spec.source).cast(pl.String)
    converted = _convert(raw, spec.kind)
    valid = converted.is_not_null()
    if spec.kind in FORMATS:
        valid &= raw.str.contains(FORMATS[spec.kind])
    invalid = raw.is_not_null() & ~valid

    parsed = (
        pl.DataFrame({"value": converted, "valid": valid})
        .select(pl.when(pl.col("valid")).then(pl.col("value")).alias(spec.name))
        .to_series()
    )
    if not invalid.any():
        return parsed, None
    bad = raw.filter(invalid)
    return parsed, Failure(table, "tipo_no_parseable", (spec.name,), bad.len(), examples(bad))


def parse_table(df: pl.DataFrame, table: str) -> tuple[pl.DataFrame, list[Failure]]:
    """Columnas del contrato de ``table`` con su tipo de Silver, y las fallas de conversión.

    El resultado tiene las columnas en el orden del contrato, con su nombre de
    Silver; las columnas de Bronze que no están en el contrato se descartan.
    """
    columns: list[pl.Series] = []
    failures: list[Failure] = []
    for spec in TABLE_COLUMNS[table]:
        column, failure = _parse_column(df, spec, table)
        columns.append(column)
        if failure is not None:
            failures.append(failure)
    return pl.DataFrame(columns), failures


def parse_tables(
    raw: Mapping[str, pl.DataFrame],
) -> tuple[dict[str, pl.DataFrame], list[Failure]]:
    """``parse_table`` sobre todas las tablas del contrato, juntando las fallas."""
    parsed: dict[str, pl.DataFrame] = {}
    failures: list[Failure] = []
    for table in TABLE_COLUMNS:
        parsed[table], table_failures = parse_table(raw[table], table)
        failures.extend(table_failures)
    return parsed, failures
