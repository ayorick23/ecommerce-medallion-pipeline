"""Validación de Silver: fallas y la excepción que las reúne (ADR 0001, 0003, 0020, 0023).

Ninguna regla lanza una excepción por su cuenta: cada una devuelve sus fallas,
y la corrida lanza una sola ``SilverValidationError`` con todas, antes de
escribir nada. Las reglas por tabla son esquemas Pandera
(``medallion.silver.schemas``); las que cruzan tablas (FK, reviews vencidas)
viven aquí, porque Pandera valida una tabla a la vez.
"""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Final, Literal

import pandera.errors
import pandera.polars as pa
import polars as pl

from medallion.silver.facts import PENDING_DAYS
from medallion.silver.schemas import PENDING_SCHEMA, PRIMARY_KEYS, SCHEMAS

MAX_EXAMPLES: Final = 5

Rule = Literal[
    "columna_faltante",
    "tipo_no_parseable",
    "nulo_no_permitido",
    "pk_duplicada",
    "fk_huerfana",
    "medida_imposible",
]


@dataclass(frozen=True)
class Failure:
    """Una regla violada en una tabla: cuántas filas y hasta 5 ejemplos."""

    table: str
    rule: Rule
    columns: tuple[str, ...]
    rows: int
    examples: tuple[str, ...] = ()

    def __str__(self) -> str:
        text = f"{self.table} [{', '.join(self.columns)}] {self.rule}: {self.rows} filas"
        if self.examples:
            text += f" (ej.: {', '.join(self.examples)})"
        return text


def examples(values: pl.Series) -> tuple[str, ...]:
    """Hasta ``MAX_EXAMPLES`` valores distintos, en orden de aparición, como texto legible."""
    return tuple(repr(v) for v in values.unique(maintain_order=True).head(MAX_EXAMPLES))


class SilverValidationError(Exception):
    """Silver no pasó la validación: hard stop total, no se escribe nada (ADR 0001)."""

    def __init__(self, failures: Sequence[Failure]) -> None:
        if not failures:
            raise ValueError("SilverValidationError requiere al menos una falla")
        self.failures: tuple[Failure, ...] = tuple(failures)
        lines = [
            f"Silver no pasó la validación: {len(self.failures)} fallas; no se escribió nada.",
            *(f"- {failure}" for failure in self.failures),
        ]
        super().__init__("\n".join(lines))


def raise_if_failed(failures: Sequence[Failure]) -> None:
    if failures:
        raise SilverValidationError(failures)


# Chequeos de Pandera que corresponden a una regla del contrato (docs/schemas.md, sección 4).
_PANDERA_RULES: Final[dict[str, Rule]] = {
    "not_nullable": "nulo_no_permitido",
    "field_uniqueness": "pk_duplicada",
    "multiple_fields_uniqueness": "pk_duplicada",
}
_MEASURE_CHECKS: Final = ("greater_than_or_equal_to", "in_range")


def _rule_for(check: str) -> Rule:
    if check in _PANDERA_RULES:
        return _PANDERA_RULES[check]
    if check.startswith(_MEASURE_CHECKS):
        return "medida_imposible"
    # Tipo o columnas distintas del esquema: Silver construyó mal la tabla.
    raise RuntimeError(f"Error de programación en Silver: chequeo {check!r} fuera del contrato")


def _row_key(df: pl.DataFrame, pk: Sequence[str], index: int) -> object:
    values = df.select(pk).row(index)
    return values[0] if len(values) == 1 else values


def schema_failures(
    schema: pa.DataFrameSchema, df: pl.DataFrame, pk: Sequence[str]
) -> list[Failure]:
    """Fallas de una tabla contra su esquema Pandera, agrupadas por regla y columna.

    Los ejemplos son el valor que falló; para nulos y PK duplicadas, la PK de la
    fila (Pandera reporta la fila completa como valor de una PK duplicada).
    """
    try:
        schema.validate(df, lazy=True)
    except pandera.errors.SchemaErrors as errors:
        cases = errors.failure_cases
    else:
        return []

    table = str(schema.name)
    failures: list[Failure] = []
    for (column, check), group in cases.group_by(["column", "check"], maintain_order=True):
        rule = _rule_for(str(check))
        by_key = rule in ("pk_duplicada", "nulo_no_permitido")
        columns = tuple(pk) if rule == "pk_duplicada" else (str(column),)
        shown = dict.fromkeys(
            repr(_row_key(df, pk, index) if by_key else case)
            for case, index in group.select("failure_case", "index").iter_rows()
        )
        failures.append(Failure(table, rule, columns, group.height, tuple(shown)[:MAX_EXAMPLES]))
    return failures


# (tabla hija, columna) → (tabla padre, columna). `order_reviews → orders` no
# está: se cumple por construcción, y su única falla posible es una review
# vencida (ADR 0020, `expired_reviews`).
FOREIGN_KEYS: Final = (
    ("orders", "customer_id", "customers", "customer_id"),
    ("order_status_history", "order_id", "orders", "order_id"),
    ("order_items", "order_id", "orders", "order_id"),
    ("order_items", "product_id", "products", "product_id"),
    ("order_items", "seller_id", "sellers", "seller_id"),
    ("order_payments", "order_id", "orders", "order_id"),
)


def fk_failures(tables: Mapping[str, pl.DataFrame]) -> list[Failure]:
    """Huérfanos de FK. Un nulo en la hija no cuenta: lo reporta la regla de nulos."""
    failures: list[Failure] = []
    for child, column, parent, parent_column in FOREIGN_KEYS:
        keys = tables[child].select(column).drop_nulls()
        known = tables[parent].select(pl.col(parent_column).alias(column))
        orphans = keys.join(known, on=column, how="anti")[column]
        if orphans.len() > 0:
            failures.append(
                Failure(child, "fk_huerfana", (column,), orphans.len(), examples(orphans))
            )
    return failures


def expired_reviews(pending: pl.DataFrame, grace_days: int) -> list[Failure]:
    """Reviews que esperaron a su pedido más de ``grace_days``: huérfanas reales (ADR 0020)."""
    expired = pending.filter(pl.col(PENDING_DAYS) > grace_days)
    if expired.height == 0:
        return []
    return [
        Failure(
            "order_reviews",
            "fk_huerfana",
            ("order_id",),
            expired.height,
            examples(expired["order_id"]),
        )
    ]


def validate_silver(
    tables: Mapping[str, pl.DataFrame], pending: pl.DataFrame, grace_days: int
) -> list[Failure]:
    """Todas las fallas de Silver ya construido: esquemas, FK y reviews vencidas."""
    failures: list[Failure] = []
    for table, schema in SCHEMAS.items():
        failures.extend(schema_failures(schema, tables[table], PRIMARY_KEYS[table]))
    failures.extend(schema_failures(PENDING_SCHEMA, pending, PRIMARY_KEYS["order_reviews"]))
    failures.extend(fk_failures(tables))
    failures.extend(expired_reviews(pending, grace_days))
    return failures
