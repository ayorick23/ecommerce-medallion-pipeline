import polars as pl
import pytest

from medallion.silver.validation import (
    Failure,
    SilverValidationError,
    examples,
    raise_if_failed,
)

PRICE = Failure("order_items", "tipo_no_parseable", ("price",), 2, ("'1.001'", "'abc'"))
CITY = Failure("sellers", "columna_faltante", ("seller_city",), 3095)


def test_failure_reads_as_one_line_with_its_examples() -> None:
    assert str(PRICE) == "order_items [price] tipo_no_parseable: 2 filas (ej.: '1.001', 'abc')"
    assert str(CITY) == "sellers [seller_city] columna_faltante: 3095 filas"


def test_error_lists_every_failure_and_keeps_them_for_inspection() -> None:
    error = SilverValidationError([PRICE, CITY])

    assert error.failures == (PRICE, CITY)
    assert str(error).splitlines() == [
        "Silver no pasó la validación: 2 fallas; no se escribió nada.",
        f"- {PRICE}",
        f"- {CITY}",
    ]


def test_error_needs_at_least_one_failure() -> None:
    with pytest.raises(ValueError, match="al menos una falla"):
        SilverValidationError([])


def test_raise_if_failed_only_raises_with_failures() -> None:
    raise_if_failed([])

    with pytest.raises(SilverValidationError) as info:
        raise_if_failed([CITY])
    assert info.value.failures == (CITY,)


def test_examples_are_distinct_in_order_and_capped() -> None:
    values = pl.Series(["b", "a", "b", "c", "d", "e", "f"])

    assert examples(values) == ("'b'", "'a'", "'c'", "'d'", "'e'")
