"""Catálogo de tablas de Bronze y sus anclas temporales (ADR 0004, docs/schemas.md)."""

from typing import Final

ORDER_PURCHASE_ANCHOR: Final = "order_purchase_timestamp"

# Timestamps de `orders` que representan eventos reales: el pedido se re-emite en
# el día de cada uno que no sea nulo.
ORDER_EVENT_ANCHORS: Final = (
    ORDER_PURCHASE_ANCHOR,
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
)

REVIEW_ANCHOR: Final = "review_creation_date"

# Sin timestamp propio: se ingieren una sola vez, el día de compra del pedido padre.
ORDER_CHILD_TABLES: Final = ("order_items", "order_payments")

# Particionadas por `dia_simulado`.
EVENT_TABLES: Final = ("orders", *ORDER_CHILD_TABLES, "order_reviews")

# Sin dimensión temporal en la fuente: snapshot completo.
REFERENCE_TABLES: Final = (
    "customers",
    "products",
    "sellers",
    "geolocation",
    "category_translation",
)
