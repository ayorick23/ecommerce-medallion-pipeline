"""Items, pagos y reviews de Silver (ADR 0004, 0018, 0020, 0021).

Ninguna tabla se filtra por su pedido: un item o un pago sin pedido llega tal
cual a la validación de FK, que frena el pipeline, en lugar de perderse en
silencio. La única excepción son las reviews cuyo pedido todavía no llegó:
esperan en ``pending`` hasta el plazo de gracia (ADR 0020).

Bronze ya ubica cada fila en su día (items y pagos con la compra, reviews con
su creación), así que leer Bronze hasta D alcanza para saber qué existe a la
fecha D; solo ``review_answer_timestamp`` se enmascara (ADR 0018).
"""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date

import polars as pl

from medallion.silver.validation import Failure, examples

PENDING_DAYS = "dias_pendiente"


@dataclass(frozen=True)
class ReviewTables:
    """Reviews con pedido (van a Silver) y reviews que esperan a su pedido."""

    reviews: pl.DataFrame
    pending: pl.DataFrame


def build_reviews(reviews: pl.DataFrame, orders: pl.DataFrame, dia: date) -> ReviewTables:
    """Separa las reviews según su pedido exista o no en ``silver_orders`` a la fecha ``dia``.

    La respuesta a una review queda nula si ocurre después de ``dia``.
    """
    answer = pl.col("review_answer_timestamp")
    masked = reviews.with_columns(
        pl.when(answer.dt.date() <= dia).then(answer).alias("review_answer_timestamp")
    )
    known = orders.select("order_id").unique()

    with_order = masked.join(known, on="order_id", how="semi")
    pending = masked.join(known, on="order_id", how="anti").with_columns(
        (pl.lit(dia) - pl.col("review_creation_date").dt.date()).dt.total_days().alias(PENDING_DAYS)
    )
    return ReviewTables(
        reviews=with_order.sort("review_id", "order_id"),
        pending=pending.sort("review_id", "order_id"),
    )


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


def build_fact_tables(
    parsed: Mapping[str, pl.DataFrame], orders: pl.DataFrame, dia: date
) -> tuple[dict[str, pl.DataFrame], pl.DataFrame]:
    """Items, pagos y reviews de Silver(``dia``), y las reviews pendientes aparte."""
    reviews = build_reviews(parsed["order_reviews"], orders, dia)
    tables = {
        "order_items": parsed["order_items"].sort("order_id", "order_item_id"),
        "order_payments": parsed["order_payments"].sort("order_id", "payment_sequential"),
        "order_reviews": reviews.reviews,
    }
    return tables, reviews.pending
