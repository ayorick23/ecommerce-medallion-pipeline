"""Contrato de tipos de Silver: qué columnas de Bronze se leen y a qué tipo se convierten.

Refleja ``docs/schemas.md``, sección 3 (ADR 0021, 0022). Las claves son las
tablas de Bronze. Solo se listan las columnas que Silver usa: cualquier otra
columna de Bronze (incluidas las de linaje) se ignora.
"""

from dataclasses import dataclass
from typing import Final, Literal

import polars as pl

Kind = Literal["text", "timestamp", "integer", "money", "coordinate", "zip_code"]

# Sin zona horaria: los timestamps de Olist son hora local de la fuente.
TIMESTAMP_DTYPE: Final = pl.Datetime("us")

# Tipo físico de Silver para cada clase de columna.
DTYPES: Final[dict[Kind, pl.DataType]] = {
    "text": pl.String(),
    "timestamp": TIMESTAMP_DTYPE,
    "integer": pl.Int64(),
    "money": pl.Decimal(18, 2),
    "coordinate": pl.Float64(),
    "zip_code": pl.String(),
}


@dataclass(frozen=True)
class ColumnSpec:
    """Una columna de Bronze, su clase de tipo y, si cambia, su nombre en Silver."""

    source: str
    kind: Kind = "text"
    rename: str | None = None

    @property
    def name(self) -> str:
        return self.rename or self.source


def _text(*names: str) -> tuple[ColumnSpec, ...]:
    return tuple(ColumnSpec(name) for name in names)


def _of(kind: Kind, *names: str) -> tuple[ColumnSpec, ...]:
    return tuple(ColumnSpec(name, kind) for name in names)


TABLE_COLUMNS: Final[dict[str, tuple[ColumnSpec, ...]]] = {
    "orders": (
        *_text("order_id", "customer_id", "order_status"),
        *_of(
            "timestamp",
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ),
    ),
    "order_items": (
        *_text("order_id"),
        ColumnSpec("order_item_id", "integer"),
        *_text("product_id", "seller_id"),
        ColumnSpec("shipping_limit_date", "timestamp"),
        *_of("money", "price", "freight_value"),
    ),
    "order_payments": (
        *_text("order_id"),
        ColumnSpec("payment_sequential", "integer"),
        *_text("payment_type"),
        ColumnSpec("payment_installments", "integer"),
        ColumnSpec("payment_value", "money"),
    ),
    "order_reviews": (
        *_text("review_id", "order_id"),
        ColumnSpec("review_score", "integer"),
        *_text("review_comment_title", "review_comment_message"),
        *_of("timestamp", "review_creation_date", "review_answer_timestamp"),
    ),
    "customers": (
        *_text("customer_id", "customer_unique_id"),
        ColumnSpec("customer_zip_code_prefix", "zip_code"),
        *_text("customer_city", "customer_state"),
    ),
    "products": (
        *_text("product_id", "product_category_name"),
        # Errata de la fuente corregida en Silver (ADR 0021).
        ColumnSpec("product_name_lenght", "integer", rename="product_name_length"),
        ColumnSpec("product_description_lenght", "integer", rename="product_description_length"),
        *_of(
            "integer",
            "product_photos_qty",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
        ),
    ),
    "sellers": (
        *_text("seller_id"),
        ColumnSpec("seller_zip_code_prefix", "zip_code"),
        *_text("seller_city", "seller_state"),
    ),
    "geolocation": (
        ColumnSpec("geolocation_zip_code_prefix", "zip_code"),
        *_of("coordinate", "geolocation_lat", "geolocation_lng"),
    ),
    "category_translation": _text("product_category_name", "product_category_name_english"),
}
