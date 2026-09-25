from datetime import date, datetime
from decimal import Decimal

import polars as pl
import pytest

from medallion.silver.facts import PENDING_DAYS, build_fact_tables, build_reviews
from medallion.silver.orders import build_order_tables
from medallion.silver.reference import build_reference
from medallion.silver.tables import DTYPES, TABLE_COLUMNS
from medallion.silver.validation import (
    Failure,
    SilverValidationError,
    examples,
    expired_reviews,
    fk_failures,
    raise_if_failed,
    validate_silver,
)

D = date(2018, 6, 30)
GRACE = 120
TS = datetime(2018, 6, 11, 12, 58, 51)

# Una fila válida por tabla de Bronze, ya tipada: un pedido completo y coherente.
BRONZE_ROWS: dict[str, dict[str, object]] = {
    "orders": {
        "order_id": "o1",
        "customer_id": "c1",
        "order_status": "delivered",
        "order_purchase_timestamp": TS,
        "order_approved_at": TS,
        "order_delivered_carrier_date": datetime(2018, 6, 12),
        "order_delivered_customer_date": datetime(2018, 6, 21),
        "order_estimated_delivery_date": datetime(2018, 7, 17),
    },
    "order_items": {
        "order_id": "o1",
        "order_item_id": 1,
        "product_id": "p1",
        "seller_id": "s1",
        "shipping_limit_date": datetime(2018, 6, 13),
        "price": Decimal("189.00"),
        "freight_value": Decimal("58.78"),
    },
    "order_payments": {
        "order_id": "o1",
        "payment_sequential": 1,
        "payment_type": "credit_card",
        "payment_installments": 2,
        "payment_value": Decimal("247.78"),
    },
    "order_reviews": {
        "review_id": "r1",
        "order_id": "o1",
        "review_score": 3,
        "review_creation_date": datetime(2018, 6, 22),
        "review_answer_timestamp": datetime(2018, 6, 22, 18, 8, 44),
    },
    "customers": {
        "customer_id": "c1",
        "customer_unique_id": "u1",
        "customer_zip_code_prefix": "77410",
        "customer_city": "gurupi",
        "customer_state": "TO",
    },
    "products": {"product_id": "p1", "product_category_name": "perfumaria"},
    "sellers": {
        "seller_id": "s1",
        "seller_zip_code_prefix": "01151",
        "seller_city": "sao paulo",
        "seller_state": "SP",
    },
    "geolocation": {
        "geolocation_zip_code_prefix": "77410",
        "geolocation_lat": -11.73,
        "geolocation_lng": -49.06,
    },
    "category_translation": {
        "product_category_name": "perfumaria",
        "product_category_name_english": "perfumery",
    },
}


def _silver() -> tuple[dict[str, pl.DataFrame], pl.DataFrame]:
    """Silver construido con los builders reales a partir de ``BRONZE_ROWS``."""
    parsed = {
        table: pl.DataFrame(
            [row], schema={spec.name: DTYPES[spec.kind] for spec in TABLE_COLUMNS[table]}
        )
        for table, row in BRONZE_ROWS.items()
    }
    orders = build_order_tables(parsed["orders"], D)
    facts, pending = build_fact_tables(parsed, orders["orders"], D)
    return {**orders, **facts, **build_reference(parsed)}, pending


def _failures(tables: dict[str, pl.DataFrame], pending: pl.DataFrame) -> list[Failure]:
    return validate_silver(tables, pending, GRACE)


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


def test_a_coherent_silver_has_no_failures() -> None:
    tables, pending = _silver()

    assert _failures(tables, pending) == []


def test_a_null_in_a_required_column_reports_the_row_key() -> None:
    tables, pending = _silver()
    tables["customers"] = tables["customers"].with_columns(
        pl.lit(None, dtype=pl.String).alias("customer_city")
    )

    assert _failures(tables, pending) == [
        Failure("customers", "nulo_no_permitido", ("customer_city",), 1, ("'c1'",))
    ]


def test_a_duplicated_primary_key_reports_every_row_involved() -> None:
    tables, pending = _silver()
    tables["order_payments"] = pl.concat([tables["order_payments"]] * 2)

    pk = ("order_id", "payment_sequential")
    assert _failures(tables, pending) == [
        Failure("order_payments", "pk_duplicada", pk, 2, ("('o1', 1)",))
    ]


def test_impossible_measures_report_the_value() -> None:
    tables, pending = _silver()
    tables["order_items"] = tables["order_items"].with_columns(
        pl.lit(Decimal("-1.00"), dtype=DTYPES["money"]).alias("price")
    )
    tables["order_reviews"] = tables["order_reviews"].with_columns(
        review_score=pl.lit(6, dtype=pl.Int64)
    )

    assert _failures(tables, pending) == [
        Failure("order_items", "medida_imposible", ("price",), 1, ("'-1.00'",)),
        Failure("order_reviews", "medida_imposible", ("review_score",), 1, ("'6'",)),
    ]


def test_zero_installments_are_valid() -> None:
    tables, pending = _silver()
    tables["order_payments"] = tables["order_payments"].with_columns(
        payment_installments=pl.lit(0, dtype=pl.Int64)
    )

    assert _failures(tables, pending) == []


def test_foreign_key_orphans_are_reported_but_nulls_are_left_to_the_null_rule() -> None:
    tables, _ = _silver()
    tables["order_items"] = tables["order_items"].with_columns(
        pl.lit("p9").alias("product_id"), pl.lit(None, dtype=pl.String).alias("seller_id")
    )

    assert fk_failures(tables) == [
        Failure("order_items", "fk_huerfana", ("product_id",), 1, ("'p9'",))
    ]


def test_a_pending_review_without_order_id_fails_now_not_after_the_grace_period() -> None:
    tables, _ = _silver()
    reviews = tables["order_reviews"].with_columns(pl.lit(None, dtype=pl.String).alias("order_id"))
    pending = build_reviews(reviews, tables["orders"], D).pending

    assert _failures(tables, pending) == [
        Failure(
            "order_reviews_pendientes", "nulo_no_permitido", ("order_id",), 1, ("('r1', None)",)
        )
    ]


def test_only_reviews_past_the_grace_period_are_orphans() -> None:
    pending = pl.DataFrame({"order_id": ["o1", "o2", "o3"], PENDING_DAYS: [120, 121, 300]})

    assert expired_reviews(pending, 120) == [
        Failure("order_reviews", "fk_huerfana", ("order_id",), 2, ("'o2'", "'o3'"))
    ]
    assert expired_reviews(pending, 300) == []


@pytest.mark.parametrize(
    "broken",
    [
        # Tipo distinto del contrato. No se usa una columna Decimal: si llega como
        # Float64, Pandera 0.32 cae con un AssertionError interno en vez de
        # reportar la falla (el pipeline se detiene igual, con otro mensaje).
        pl.col("order_item_id").cast(pl.Int32),
        pl.lit("x").alias("columna_extra"),  # columna fuera del contrato
    ],
    ids=["tipo", "columna-extra"],
)
def test_a_table_built_outside_the_contract_is_a_programming_error(broken: pl.Expr) -> None:
    tables, pending = _silver()
    tables["order_items"] = tables["order_items"].with_columns(broken)

    with pytest.raises(RuntimeError, match="Error de programación en Silver"):
        _failures(tables, pending)
