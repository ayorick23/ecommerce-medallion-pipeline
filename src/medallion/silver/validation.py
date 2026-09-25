"""Fallas de validación de Silver y la excepción que las reúne (ADR 0001, 0003, 0023).

Ninguna regla lanza una excepción por su cuenta: cada una devuelve sus fallas,
y la corrida lanza una sola ``SilverValidationError`` con todas, antes de
escribir nada.
"""

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Final, Literal

import polars as pl

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
