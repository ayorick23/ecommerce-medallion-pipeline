"""Ruteo de filas crudas a su ``dia_simulado`` (ADR 0004, 0011).

Funciones puras: reciben DataFrames con las columnas fuente como texto y
devuelven el subconjunto de filas de un día, sin modificar ningún valor.
Una fila que no se puede ubicar en el tiempo hace fallar la ingesta
(``RoutingError``): perderla en silencio sería peor.
"""

import functools
import operator
from datetime import date

import polars as pl

from medallion.bronze.tables import ORDER_EVENT_ANCHORS, ORDER_PURCHASE_ANCHOR, REVIEW_ANCHOR

TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"
_MAX_EXAMPLES = 5

_PARENT_DAY = "_dia_pedido_padre"


class RoutingError(ValueError):
    """Hay filas que no se pueden asignar a un ``dia_simulado``."""


def anchor_day(df: pl.DataFrame, column: str, *, required: bool = False) -> pl.Series:
    """Día (``Date``) de un timestamp ancla guardado como texto.

    El parseo es estricto: un valor no nulo que no cumple ``TIMESTAMP_FORMAT``
    es un error. Los nulos se conservan, salvo que ``required`` sea verdadero.
    """
    raw = df.get_column(column)
    if required and raw.null_count() > 0:
        raise RoutingError(
            f"{column}: {raw.null_count()} valores nulos en un ancla obligatoria; "
            "esas filas no se pueden ubicar en ningún día"
        )
    parsed = raw.str.strptime(pl.Datetime, format=TIMESTAMP_FORMAT, strict=False)
    invalid = raw.filter(raw.is_not_null() & parsed.is_null())
    if invalid.len() > 0:
        raise RoutingError(
            f"{column}: {invalid.len()} valores no cumplen el formato {TIMESTAMP_FORMAT!r} "
            f"(ej.: {invalid.head(_MAX_EXAMPLES).to_list()})"
        )
    return parsed.dt.date().alias(column)


def orders_for_day(orders: pl.DataFrame, dia: date) -> pl.DataFrame:
    """Pedidos con al menos un evento ancla en ``dia``.

    Una sola fila por pedido aunque varios de sus eventos caigan ese día: el
    grano de ``bronze_orders`` es ``(order_id, dia_simulado)``.
    """
    matches = [
        (anchor_day(orders, column, required=column == ORDER_PURCHASE_ANCHOR) == dia).fill_null(
            False
        )
        for column in ORDER_EVENT_ANCHORS
    ]
    return orders.filter(functools.reduce(operator.or_, matches))


def reviews_for_day(reviews: pl.DataFrame, dia: date) -> pl.DataFrame:
    """Reviews cuya ``review_creation_date`` cae en ``dia``."""
    return reviews.filter(anchor_day(reviews, REVIEW_ANCHOR, required=True) == dia)


def order_children_for_day(
    children: pl.DataFrame, orders: pl.DataFrame, dia: date, *, table: str
) -> pl.DataFrame:
    """Filas de una tabla hija de ``orders`` (items, pagos) del día de compra de su pedido.

    Se ingieren una sola vez, junto al día de ``order_purchase_timestamp`` del
    pedido padre; no se re-emiten con los eventos posteriores del pedido.
    """
    parents = orders.select("order_id").with_columns(
        anchor_day(orders, ORDER_PURCHASE_ANCHOR, required=True).alias(_PARENT_DAY)
    )

    ambiguous = parents.unique().filter(pl.col("order_id").is_duplicated())
    if ambiguous.height > 0:
        raise RoutingError(
            f"{table}: {ambiguous['order_id'].n_unique()} pedidos padre con más de un día "
            f"de compra (ej.: {ambiguous['order_id'].unique().head(_MAX_EXAMPLES).to_list()})"
        )

    orphans = children.join(parents, on="order_id", how="anti")
    if orphans.height > 0:
        raise RoutingError(
            f"{table}: {orphans.height} filas sin pedido padre en orders "
            f"(ej.: {orphans['order_id'].head(_MAX_EXAMPLES).to_list()})"
        )

    day_parents = parents.filter(pl.col(_PARENT_DAY) == dia).select("order_id")
    return children.join(day_parents, on="order_id", how="semi", maintain_order="left")
