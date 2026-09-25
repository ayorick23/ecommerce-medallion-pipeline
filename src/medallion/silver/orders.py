"""Pedidos de Silver y su historial de estados SCD2 (ADR 0004, 0018, 0019).

``silver_order_status_history`` despliega los 4 timestamps de cada pedido en
eventos ordenados por **etapa**, con un ``valid_from`` que nunca retrocede.
``silver_orders`` es la fila del pedido con sus timestamps enmascarados según
los eventos visibles del historial, para que ambas tablas nunca se
contradigan.
"""

from datetime import date
from typing import Final

import polars as pl

# Etapas del pedido en su orden lógico, con el timestamp de Olist que las fecha.
STAGES: Final = (
    ("creado", "order_purchase_timestamp"),
    ("aprobado", "order_approved_at"),
    ("despachado", "order_delivered_carrier_date"),
    ("entregado", "order_delivered_customer_date"),
)

_STAGE = "_etapa"


def collapse_reemissions(orders: pl.DataFrame) -> pl.DataFrame:
    """Una fila por pedido a partir de sus re-emisiones de Bronze (ADR 0004).

    Solo colapsa filas idénticas. Si dos re-emisiones de un mismo pedido
    difieren, ambas sobreviven y la validación de PK frena el pipeline: no se
    elige una en silencio.
    """
    return orders.unique()


def build_status_history(orders: pl.DataFrame, dia: date) -> pl.DataFrame:
    """Historial SCD2 de estados, con los eventos visibles al final de ``dia``.

    ``valid_from`` = máximo acumulado de los timestamps crudos en orden de
    etapa (ADR 0019). Como nunca retrocede, los eventos visibles a la fecha D
    son siempre un prefijo del historial completo del pedido (ADR 0018).
    """
    events = pl.concat(
        [
            orders.select(
                "order_id",
                pl.lit(event).alias("status_event"),
                pl.lit(stage, dtype=pl.Int8).alias(_STAGE),
                pl.col("order_status").alias("order_status_raw"),
                pl.col(column).alias("event_timestamp"),
            )
            for stage, (event, column) in enumerate(STAGES, start=1)
        ]
    ).drop_nulls("event_timestamp")

    return (
        events.sort("order_id", _STAGE)
        .with_columns(pl.col("event_timestamp").cum_max().over("order_id").alias("valid_from"))
        .filter(pl.col("valid_from").dt.date() <= dia)
        .with_columns(pl.col("valid_from").shift(-1).over("order_id").alias("valid_to"))
        .select(
            "order_id",
            "status_event",
            "order_status_raw",
            "event_timestamp",
            "valid_from",
            "valid_to",
            pl.col("valid_to").is_null().alias("is_current"),
            (pl.col("valid_from") != pl.col("event_timestamp")).alias("is_adjusted"),
        )
    )


def build_orders(orders: pl.DataFrame, history: pl.DataFrame, dia: date) -> pl.DataFrame:
    """Pedidos comprados hasta ``dia``, con los timestamps de eventos no visibles en nulo.

    Un pedido sin fecha de compra no se descarta: queda para que la validación
    lo reporte como nulo no permitido en lugar de perderse en silencio.
    """
    purchase = pl.col("order_purchase_timestamp")
    visible = orders.filter((purchase.dt.date() <= dia) | purchase.is_null())

    for event, column in STAGES[1:]:
        flag = f"_{event}_visible"
        seen = (
            history.filter(pl.col("status_event") == event)
            .select("order_id", pl.lit(True).alias(flag))
            .unique()
        )
        visible = (
            visible.join(seen, on="order_id", how="left")
            .with_columns(pl.when(pl.col(flag)).then(pl.col(column)).alias(column))
            .drop(flag)
        )

    return visible.select(orders.columns).sort("order_id")


def build_order_tables(orders: pl.DataFrame, dia: date) -> dict[str, pl.DataFrame]:
    """``orders`` y ``order_status_history`` de Silver(``dia``), desde Bronze ya tipado."""
    collapsed = collapse_reemissions(orders)
    history = build_status_history(collapsed, dia)
    return {"orders": build_orders(collapsed, history, dia), "order_status_history": history}
