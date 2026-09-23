from datetime import date

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from bronze.routing import (
    RoutingError,
    anchor_day,
    order_children_for_day,
    orders_for_day,
    reviews_for_day,
)

DIA = date(2017, 3, 15)

ORDER_COLUMNS = [
    "order_id",
    "customer_id",
    "order_status",
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
]


def _orders(*rows: tuple[str | None, ...]) -> pl.DataFrame:
    return pl.DataFrame(rows, schema=dict.fromkeys(ORDER_COLUMNS, pl.String), orient="row")


def _order(
    order_id: str,
    purchase: str | None,
    approved: str | None = None,
    carrier: str | None = None,
    delivered: str | None = None,
) -> tuple[str | None, ...]:
    return (
        order_id,
        f"cust-{order_id}",
        "delivered",
        purchase,
        approved,
        carrier,
        delivered,
        "2017-04-01 00:00:00",
    )


def _ids(df: pl.DataFrame) -> list[str]:
    return sorted(df["order_id"].to_list())


# --- anchor_day --------------------------------------------------------------


def test_anchor_day_parses_to_date_and_keeps_nulls() -> None:
    df = pl.DataFrame({"ts": ["2017-03-15 23:59:59", None]})

    days = anchor_day(df, "ts")

    assert days.dtype == pl.Date
    assert days.to_list() == [DIA, None]


@pytest.mark.parametrize(
    "bad_value",
    ["2017-03-15", "15/03/2017 10:00:00", "2017-03-15 10:00:00.123", "", "no-es-fecha"],
)
def test_anchor_day_rejects_values_outside_the_strict_format(bad_value: str) -> None:
    df = pl.DataFrame({"ts": ["2017-03-15 10:00:00", bad_value]})

    with pytest.raises(RoutingError, match="ts: 1 valores no cumplen el formato"):
        anchor_day(df, "ts")


def test_anchor_day_required_rejects_nulls() -> None:
    df = pl.DataFrame({"ts": ["2017-03-15 10:00:00", None]}, schema={"ts": pl.String})

    with pytest.raises(RoutingError, match="ancla obligatoria"):
        anchor_day(df, "ts", required=True)


# --- orders_for_day ----------------------------------------------------------


def test_orders_routed_by_any_event_anchor() -> None:
    orders = _orders(
        _order("compra-hoy", "2017-03-15 08:00:00"),
        _order("aprobado-hoy", "2017-03-14 22:00:00", approved="2017-03-15 01:00:00"),
        _order("despachado-hoy", "2017-03-10 10:00:00", carrier="2017-03-15 12:00:00"),
        _order(
            "entregado-hoy",
            "2017-03-01 10:00:00",
            approved="2017-03-01 11:00:00",
            carrier="2017-03-03 10:00:00",
            delivered="2017-03-15 18:00:00",
        ),
        _order("otro-dia", "2017-03-14 10:00:00", approved="2017-03-16 10:00:00"),
    )

    result = orders_for_day(orders, DIA)

    assert _ids(result) == ["aprobado-hoy", "compra-hoy", "despachado-hoy", "entregado-hoy"]


def test_several_events_same_day_emit_a_single_row() -> None:
    orders = _orders(
        _order(
            "todo-hoy",
            "2017-03-15 08:00:00",
            approved="2017-03-15 08:10:00",
            carrier="2017-03-15 15:00:00",
        )
    )

    assert orders_for_day(orders, DIA).height == 1


def test_orders_rows_are_copied_unchanged() -> None:
    orders = _orders(_order("a", "2017-03-15 08:00:00", approved="2017-03-15 09:00:00"))

    assert_frame_equal(orders_for_day(orders, DIA), orders)


def test_order_with_null_optional_anchors_is_routed_by_the_rest() -> None:
    orders = _orders(_order("cancelado", "2017-03-15 08:00:00"))

    assert _ids(orders_for_day(orders, DIA)) == ["cancelado"]


def test_day_without_orders_returns_empty_frame_with_same_schema() -> None:
    orders = _orders(_order("a", "2017-03-14 08:00:00"))

    result = orders_for_day(orders, DIA)

    assert result.height == 0
    assert result.schema == orders.schema


def test_order_without_purchase_timestamp_fails() -> None:
    orders = _orders(_order("sin-compra", None, approved="2017-03-15 09:00:00"))

    with pytest.raises(RoutingError, match="order_purchase_timestamp"):
        orders_for_day(orders, DIA)


def test_malformed_optional_anchor_fails_even_if_other_anchor_matches() -> None:
    orders = _orders(_order("a", "2017-03-15 08:00:00", approved="ayer"))

    with pytest.raises(RoutingError, match="order_approved_at"):
        orders_for_day(orders, DIA)


# --- reviews_for_day ---------------------------------------------------------


def _reviews(*rows: tuple[str, str, str | None]) -> pl.DataFrame:
    return pl.DataFrame(
        rows,
        schema={"review_id": pl.String, "order_id": pl.String, "review_creation_date": pl.String},
        orient="row",
    )


def test_reviews_routed_by_creation_date() -> None:
    reviews = _reviews(
        ("r1", "a", "2017-03-15 00:00:00"),
        ("r2", "b", "2017-03-16 00:00:00"),
    )

    assert reviews_for_day(reviews, DIA)["review_id"].to_list() == ["r1"]


def test_review_without_creation_date_fails() -> None:
    reviews = _reviews(("r1", "a", None))

    with pytest.raises(RoutingError, match="review_creation_date"):
        reviews_for_day(reviews, DIA)


# --- order_children_for_day --------------------------------------------------


def _items(*order_ids: str) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "order_id": list(order_ids),
            "order_item_id": [str(i) for i in range(1, len(order_ids) + 1)],
            "price": ["10.50"] * len(order_ids),
        }
    )


def test_children_follow_parent_purchase_day_only() -> None:
    orders = _orders(
        _order("comprado-hoy", "2017-03-15 08:00:00"),
        # Aprobado hoy pero comprado ayer: el pedido se re-emite hoy, sus items no.
        _order("aprobado-hoy", "2017-03-14 08:00:00", approved="2017-03-15 08:00:00"),
    )
    items = _items("comprado-hoy", "comprado-hoy", "aprobado-hoy")

    result = order_children_for_day(items, orders, DIA, table="order_items")

    assert result["order_id"].to_list() == ["comprado-hoy", "comprado-hoy"]
    assert result.columns == items.columns


def test_orphan_child_fails() -> None:
    orders = _orders(_order("a", "2017-03-15 08:00:00"))
    items = _items("a", "huerfano")

    with pytest.raises(RoutingError, match="order_items: 1 filas sin pedido padre"):
        order_children_for_day(items, orders, DIA, table="order_items")


def test_parent_with_two_purchase_days_fails() -> None:
    orders = _orders(
        _order("dup", "2017-03-15 08:00:00"),
        _order("dup", "2017-03-16 08:00:00"),
    )

    with pytest.raises(RoutingError, match="más de un día de compra"):
        order_children_for_day(_items("dup"), orders, DIA, table="order_payments")
