from collections.abc import Sequence
from datetime import datetime
from decimal import Decimal

import polars as pl
import pytest

from medallion.silver.parsing import parse_table, parse_tables
from medallion.silver.tables import DTYPES, TABLE_COLUMNS, Kind
from medallion.silver.validation import Failure

VALID: dict[Kind, str] = {
    "text": "x",
    "timestamp": "2017-10-02 10:56:33",
    "integer": "1",
    "money": "1.00",
    "coordinate": "-23.5",
    "zip_code": "01151",
}


def _raw(table: str, rows: int = 1, **overrides: Sequence[str | None]) -> pl.DataFrame:
    """Frame de Bronze (todo texto) con valores válidos, salvo las columnas indicadas."""
    data = {
        spec.source: list(overrides.get(spec.source, [VALID[spec.kind]] * rows))
        for spec in TABLE_COLUMNS[table]
    }
    return pl.DataFrame(data, schema=dict.fromkeys(data, pl.String))


def _only_failure(failures: list[Failure]) -> Failure:
    assert len(failures) == 1, failures
    return failures[0]


@pytest.mark.parametrize("table", list(TABLE_COLUMNS))
def test_valid_rows_get_the_contract_columns_and_types(table: str) -> None:
    raw = _raw(table).with_columns(pl.lit("2017-03-15").alias("dia_simulado"))

    parsed, failures = parse_table(raw, table)

    assert failures == []
    assert parsed.schema == pl.Schema(
        {spec.name: DTYPES[spec.kind] for spec in TABLE_COLUMNS[table]}
    )
    assert parsed.null_count().row(0) == (0,) * parsed.width


def test_misspelled_product_columns_are_renamed() -> None:
    parsed, _ = parse_table(_raw("products"), "products")

    assert "product_name_length" in parsed.columns
    assert "product_description_length" in parsed.columns
    assert not any("lenght" in column for column in parsed.columns)


def test_nulls_are_kept_and_are_not_a_parsing_failure() -> None:
    raw = _raw("orders", 2, order_approved_at=[None, "2017-10-02 11:07:15"])

    parsed, failures = parse_table(raw, "orders")

    assert failures == []
    assert parsed["order_approved_at"].to_list() == [None, datetime(2017, 10, 2, 11, 7, 15)]


def test_money_is_exact_and_rejects_more_than_two_decimals() -> None:
    values = ["10.10", "7", "-5.00", "12.345", "1,50", "abc"]
    raw = _raw("order_payments", len(values), payment_value=values)

    parsed, failures = parse_table(raw, "order_payments")

    assert parsed["payment_value"].to_list() == [
        Decimal("10.10"),
        Decimal("7.00"),
        Decimal("-5.00"),  # el formato es válido; el rango lo valida otra regla
        None,  # no se trunca a 12.34
        None,
        None,
    ]
    failure = _only_failure(failures)
    assert failure == Failure(
        "order_payments",
        "tipo_no_parseable",
        ("payment_value",),
        3,
        ("'12.345'", "'1,50'", "'abc'"),
    )


def test_timestamps_need_the_exact_format_and_a_valid_date() -> None:
    values = ["2017-10-02 10:56:33", "2017-13-01 00:00:00", "2017-10-02", "2017-10-2 10:56:33"]
    raw = _raw("order_items", len(values), shipping_limit_date=values)

    parsed, failures = parse_table(raw, "order_items")

    assert (
        parsed["shipping_limit_date"].to_list() == [datetime(2017, 10, 2, 10, 56, 33)] + [None] * 3
    )
    assert _only_failure(failures).rows == 3


def test_zip_codes_keep_leading_zeros_and_need_five_digits() -> None:
    values = ["01151", "1151", "1151a"]
    raw = _raw("customers", len(values), customer_zip_code_prefix=values)

    parsed, failures = parse_table(raw, "customers")

    assert parsed["customer_zip_code_prefix"].to_list() == ["01151", None, None]
    assert _only_failure(failures).examples == ("'1151'", "'1151a'")


def test_integers_accept_a_sign_but_not_decimals() -> None:
    values = ["-1", "0", "1.0"]
    raw = _raw("order_payments", len(values), payment_installments=values)

    parsed, failures = parse_table(raw, "order_payments")

    assert parsed["payment_installments"].to_list() == [-1, 0, None]
    assert _only_failure(failures).examples == ("'1.0'",)


def test_coordinates_reject_scientific_notation() -> None:
    values = ["-23.54562128115268", "-46", "1e5"]
    raw = _raw("geolocation", len(values), geolocation_lat=values)

    parsed, failures = parse_table(raw, "geolocation")

    assert parsed["geolocation_lat"].to_list() == [-23.54562128115268, -46.0, None]
    assert _only_failure(failures).rule == "tipo_no_parseable"


def test_a_missing_column_is_a_failure_and_the_column_comes_back_null() -> None:
    raw = _raw("sellers", 2).drop("seller_city")

    parsed, failures = parse_table(raw, "sellers")

    assert _only_failure(failures) == Failure("sellers", "columna_faltante", ("seller_city",), 2)
    assert parsed["seller_city"].dtype == pl.String
    assert parsed["seller_city"].null_count() == 2


def test_examples_are_distinct_and_capped_at_five() -> None:
    values = ["a", "a", "b", "c", "d", "e", "f", "g"]
    raw = _raw("order_reviews", len(values), review_score=values)

    _, failures = parse_table(raw, "order_reviews")

    failure = _only_failure(failures)
    assert failure.rows == 8
    assert failure.examples == ("'a'", "'b'", "'c'", "'d'", "'e'")


def test_parse_tables_collects_failures_from_every_table() -> None:
    raw = {table: _raw(table) for table in TABLE_COLUMNS}
    raw["order_items"] = _raw("order_items", price=["1.001"])
    raw["sellers"] = _raw("sellers", seller_zip_code_prefix=["123"])

    parsed, failures = parse_tables(raw)

    assert set(parsed) == set(TABLE_COLUMNS)
    assert [(f.table, f.columns) for f in failures] == [
        ("order_items", ("price",)),
        ("sellers", ("seller_zip_code_prefix",)),
    ]
