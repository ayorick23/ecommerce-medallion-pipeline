"""SCD2 de estados y enmascarado de pedidos, con pedidos reales de Olist como casos."""

from datetime import date, datetime

import polars as pl

from medallion.silver.orders import build_order_tables
from medallion.silver.tables import DTYPES, TABLE_COLUMNS

# Pedido 03ecec24…: comprado el 11/06/2018, entregado el 21/06.
NORMAL = {
    "order_id": "03ecec24",
    "order_purchase_timestamp": datetime(2018, 6, 11, 12, 58, 51),
    "order_approved_at": datetime(2018, 6, 11, 20, 23, 8),
    "order_delivered_carrier_date": datetime(2018, 6, 12, 14, 35, 0),
    "order_delivered_customer_date": datetime(2018, 6, 21, 11, 37, 48),
}

# Pedido 000576fe…: despachado antes de aprobarse.
SHIPPED_BEFORE_APPROVAL = {
    "order_id": "000576fe",
    "order_purchase_timestamp": datetime(2018, 7, 4, 12, 8, 27),
    "order_approved_at": datetime(2018, 7, 5, 16, 35, 48),
    "order_delivered_carrier_date": datetime(2018, 7, 5, 12, 15, 0),
    "order_delivered_customer_date": datetime(2018, 7, 9, 14, 4, 7),
}

# Pedido 7c48bb55…: despacho con fecha de 6 meses antes de la compra.
SHIPPED_BEFORE_PURCHASE = {
    "order_id": "7c48bb55",
    "order_purchase_timestamp": datetime(2018, 7, 16, 18, 40, 53),
    "order_approved_at": datetime(2018, 7, 16, 18, 50, 22),
    "order_delivered_carrier_date": datetime(2018, 1, 26, 13, 35, 0),
    "order_delivered_customer_date": None,
}

HISTORY_COLUMNS = ["status_event", "event_timestamp", "valid_from", "valid_to", "is_adjusted"]


def _orders(*orders: dict[str, object], status: str = "delivered") -> pl.DataFrame:
    """Frame de ``orders`` de Bronze ya tipado; timestamps ausentes quedan nulos."""
    schema = {spec.name: DTYPES[spec.kind] for spec in TABLE_COLUMNS["orders"]}
    rows = [
        {
            "customer_id": "c-" + str(order["order_id"]),
            "order_status": status,
            "order_estimated_delivery_date": datetime(2018, 12, 31),
            **order,
        }
        for order in orders
    ]
    return pl.DataFrame(rows, schema=schema)


def _history(orders: pl.DataFrame, dia: date, order_id: str) -> list[tuple[object, ...]]:
    history = build_order_tables(orders, dia)["order_status_history"]
    return history.filter(pl.col("order_id") == order_id).select(HISTORY_COLUMNS).rows()


def test_reemissions_collapse_into_one_order() -> None:
    reemitted = _orders(NORMAL, NORMAL, NORMAL)  # días 11/06, 12/06 y 21/06 en Bronze

    orders = build_order_tables(reemitted, date(2018, 6, 30))["orders"]

    assert orders.height == 1


def test_the_purchase_day_only_knows_creation_and_approval() -> None:
    tables = build_order_tables(_orders(NORMAL), date(2018, 6, 11))

    assert _history(_orders(NORMAL), date(2018, 6, 11), "03ecec24") == [
        ("creado", NORMAL["order_purchase_timestamp"], NORMAL["order_purchase_timestamp"],
         NORMAL["order_approved_at"], False),
        ("aprobado", NORMAL["order_approved_at"], NORMAL["order_approved_at"], None, False),
    ]  # fmt: skip
    order = tables["orders"].row(0, named=True)
    assert order["order_approved_at"] == NORMAL["order_approved_at"]
    assert order["order_delivered_carrier_date"] is None
    assert order["order_delivered_customer_date"] is None
    assert order["order_status"] == "delivered"  # estado final de la fuente (ADR 0018)


def test_each_day_closes_the_current_event_and_adds_the_new_one() -> None:
    history = build_order_tables(_orders(NORMAL), date(2018, 6, 12))["order_status_history"]
    assert history.select("status_event", "is_current").rows() == [
        ("creado", False),
        ("aprobado", False),
        ("despachado", True),
    ]

    tables = build_order_tables(_orders(NORMAL), date(2018, 6, 22))
    closed = tables["order_status_history"].select("status_event", "valid_to", "is_current")
    assert closed.rows() == [
        ("creado", NORMAL["order_approved_at"], False),
        ("aprobado", NORMAL["order_delivered_carrier_date"], False),
        ("despachado", NORMAL["order_delivered_customer_date"], False),
        ("entregado", None, True),
    ]  # fmt: skip
    assert tables["orders"].row(0, named=True) == {
        "customer_id": "c-03ecec24",
        "order_status": "delivered",
        "order_estimated_delivery_date": datetime(2018, 12, 31),
        **NORMAL,
    }


def test_an_order_does_not_exist_before_its_purchase_day() -> None:
    tables = build_order_tables(_orders(NORMAL), date(2018, 6, 10))

    assert tables["orders"].height == 0
    assert tables["order_status_history"].height == 0


def test_valid_from_never_goes_back_and_keeps_the_raw_timestamp() -> None:
    approved = SHIPPED_BEFORE_APPROVAL["order_approved_at"]

    rows = _history(_orders(SHIPPED_BEFORE_APPROVAL), date(2018, 7, 31), "000576fe")

    assert rows == [
        ("creado", SHIPPED_BEFORE_APPROVAL["order_purchase_timestamp"],
         SHIPPED_BEFORE_APPROVAL["order_purchase_timestamp"], approved, False),
        ("aprobado", approved, approved, approved, False),  # duración cero
        ("despachado", SHIPPED_BEFORE_APPROVAL["order_delivered_carrier_date"], approved,
         SHIPPED_BEFORE_APPROVAL["order_delivered_customer_date"], True),
        ("entregado", SHIPPED_BEFORE_APPROVAL["order_delivered_customer_date"],
         SHIPPED_BEFORE_APPROVAL["order_delivered_customer_date"], None, False),
    ]  # fmt: skip


def test_an_event_dated_before_the_purchase_waits_for_the_purchase_day() -> None:
    orders = _orders(SHIPPED_BEFORE_PURCHASE)

    before = build_order_tables(orders, date(2018, 7, 15))
    assert before["orders"].height == 0  # el despacho "del 26/01" no lo hace aparecer
    assert before["order_status_history"].height == 0

    on_purchase = build_order_tables(orders, date(2018, 7, 16))
    assert on_purchase["orders"]["order_delivered_carrier_date"].to_list() == [
        SHIPPED_BEFORE_PURCHASE["order_delivered_carrier_date"]  # valor crudo, visible
    ]
    history = on_purchase["order_status_history"]
    assert history.select("status_event", "valid_from", "is_current", "is_adjusted").rows()[-1] == (
        "despachado",
        SHIPPED_BEFORE_PURCHASE["order_approved_at"],
        True,
        True,
    )


def test_a_skipped_stage_generates_no_row() -> None:
    order = {**NORMAL, "order_approved_at": None}

    rows = _history(_orders(order), date(2018, 6, 30), "03ecec24")

    assert [row[0] for row in rows] == ["creado", "despachado", "entregado"]
    assert rows[0][3] == NORMAL["order_delivered_carrier_date"]  # creado cierra con el despacho


def test_delivery_before_shipping_still_ends_as_delivered() -> None:
    order = {
        **NORMAL,
        "order_delivered_carrier_date": datetime(2018, 6, 20, 9, 0, 0),
        "order_delivered_customer_date": datetime(2018, 6, 19, 15, 0, 0),
    }

    rows = _history(_orders(order), date(2018, 6, 30), "03ecec24")

    assert rows[-1] == (
        "entregado",
        datetime(2018, 6, 19, 15, 0, 0),
        datetime(2018, 6, 20, 9, 0, 0),
        None,
        True,
    )


def test_a_canceled_order_keeps_its_final_status_as_context() -> None:
    order = {"order_id": "00310b0c", **{k: v for k, v in NORMAL.items() if k != "order_id"}}
    order |= {"order_delivered_carrier_date": None, "order_delivered_customer_date": None}

    tables = build_order_tables(_orders(order, status="canceled"), date(2018, 6, 30))

    history = tables["order_status_history"]
    assert history.select("status_event", "order_status_raw", "is_current").rows() == [
        ("creado", "canceled", False),
        ("aprobado", "canceled", True),
    ]


def test_conflicting_reemissions_are_not_collapsed() -> None:
    changed = {**NORMAL, "order_delivered_customer_date": datetime(2018, 6, 25)}

    orders = build_order_tables(_orders(NORMAL, changed), date(2018, 6, 30))["orders"]

    assert orders.height == 2  # la validación de PK lo reporta (paso 6)


def test_an_order_without_purchase_date_is_kept_for_validation() -> None:
    order = {**NORMAL, "order_purchase_timestamp": None}

    orders = build_order_tables(_orders(order), date(2018, 6, 30))["orders"]

    assert orders["order_id"].to_list() == ["03ecec24"]


def test_tables_are_sorted_and_keep_the_contract_columns() -> None:
    second = {**NORMAL, "order_id": "00000000"}

    tables = build_order_tables(_orders(NORMAL, second), date(2018, 6, 30))

    assert tables["orders"].columns == [spec.name for spec in TABLE_COLUMNS["orders"]]
    assert tables["orders"]["order_id"].to_list() == ["00000000", "03ecec24"]
    history = tables["order_status_history"]
    assert history.columns == [
        "order_id",
        "status_event",
        "order_status_raw",
        "event_timestamp",
        "valid_from",
        "valid_to",
        "is_current",
        "is_adjusted",
    ]
    assert history["order_id"].to_list() == ["00000000"] * 4 + ["03ecec24"] * 4
