"""Esquemas Pandera de las tablas de Silver (docs/schemas.md, secciones 3 y 4).

Declaran tipos, nulos, rangos y PK de cada tabla. Son estrictos y ordenados:
una columna de más, de menos o fuera de lugar es un error de programación de
Silver, no un dato roto (ver ``validation.schema_failures``).
"""

from typing import Final

import pandera.polars as pa
import polars as pl

from medallion.silver.facts import PENDING_DAYS
from medallion.silver.tables import DTYPES

STRING: Final = pl.String
INTEGER: Final = pl.Int64
BOOLEAN: Final = pl.Boolean
TIMESTAMP: Final = DTYPES["timestamp"]
MONEY: Final = DTYPES["money"]
COORDINATE: Final = pl.Float64

NON_NEGATIVE: Final = pa.Check.ge(0)

# PK de cada tabla de Silver (docs/schemas.md, sección 3).
PRIMARY_KEYS: Final[dict[str, tuple[str, ...]]] = {
    "orders": ("order_id",),
    "order_status_history": ("order_id", "status_event"),
    "order_items": ("order_id", "order_item_id"),
    "order_payments": ("order_id", "payment_sequential"),
    "order_reviews": ("review_id", "order_id"),
    "customers": ("customer_id",),
    "products": ("product_id",),
    "sellers": ("seller_id",),
    "geolocation_agg": ("geolocation_zip_code_prefix",),
    "category_translation": ("product_category_name",),
}

PENDING_REVIEWS: Final = "order_reviews_pendientes"


PolarsType = pl.DataType | type[pl.DataType]


def _required(dtype: PolarsType, *checks: pa.Check) -> pa.Column:
    return pa.Column(dtype, list(checks), nullable=False)


def _optional(dtype: PolarsType) -> pa.Column:
    return pa.Column(dtype, nullable=True)


def _schema(name: str, pk: tuple[str, ...], columns: dict[str, pa.Column]) -> pa.DataFrameSchema:
    return pa.DataFrameSchema(columns, unique=list(pk), name=name, strict=True, ordered=True)


_REVIEW_COLUMNS: Final = {
    "review_id": _required(STRING),
    "order_id": _required(STRING),
    "review_score": _required(INTEGER, pa.Check.in_range(1, 5)),
    "review_comment_title": _optional(STRING),
    "review_comment_message": _optional(STRING),
    "review_creation_date": _required(TIMESTAMP),
    "review_answer_timestamp": _optional(TIMESTAMP),
}

_COLUMNS: Final[dict[str, dict[str, pa.Column]]] = {
    "orders": {
        "order_id": _required(STRING),
        "customer_id": _required(STRING),
        "order_status": _required(STRING),
        "order_purchase_timestamp": _required(TIMESTAMP),
        "order_approved_at": _optional(TIMESTAMP),
        "order_delivered_carrier_date": _optional(TIMESTAMP),
        "order_delivered_customer_date": _optional(TIMESTAMP),
        "order_estimated_delivery_date": _required(TIMESTAMP),
    },
    "order_status_history": {
        "order_id": _required(STRING),
        "status_event": _required(STRING),
        "order_status_raw": _required(STRING),
        "event_timestamp": _required(TIMESTAMP),
        "valid_from": _required(TIMESTAMP),
        "valid_to": _optional(TIMESTAMP),
        "is_current": _required(BOOLEAN),
        "is_adjusted": _required(BOOLEAN),
    },
    "order_items": {
        "order_id": _required(STRING),
        "order_item_id": _required(INTEGER),
        "product_id": _required(STRING),
        "seller_id": _required(STRING),
        "shipping_limit_date": _required(TIMESTAMP),
        "price": _required(MONEY, NON_NEGATIVE),
        "freight_value": _required(MONEY, NON_NEGATIVE),
    },
    "order_payments": {
        "order_id": _required(STRING),
        "payment_sequential": _required(INTEGER),
        "payment_type": _required(STRING),
        "payment_installments": _required(INTEGER, NON_NEGATIVE),  # ADR 0021
        "payment_value": _required(MONEY, NON_NEGATIVE),
    },
    "order_reviews": _REVIEW_COLUMNS,
    "customers": {
        "customer_id": _required(STRING),
        "customer_unique_id": _required(STRING),
        "customer_zip_code_prefix": _required(STRING),
        "customer_city": _required(STRING),
        "customer_state": _required(STRING),
    },
    "products": {
        "product_id": _required(STRING),
        "product_category_name": _required(STRING),
        "product_category_name_english": _required(STRING),
        # Huecos de catálogo: nullable, sin fail-fast (ADR 0003).
        "product_name_length": _optional(INTEGER),
        "product_description_length": _optional(INTEGER),
        "product_photos_qty": _optional(INTEGER),
        "product_weight_g": _optional(INTEGER),
        "product_length_cm": _optional(INTEGER),
        "product_height_cm": _optional(INTEGER),
        "product_width_cm": _optional(INTEGER),
    },
    "sellers": {
        "seller_id": _required(STRING),
        "seller_zip_code_prefix": _required(STRING),
        "seller_city": _required(STRING),
        "seller_state": _required(STRING),
    },
    "geolocation_agg": {
        "geolocation_zip_code_prefix": _required(STRING),
        "geolocation_lat": _required(COORDINATE),
        "geolocation_lng": _required(COORDINATE),
    },
    "category_translation": {
        "product_category_name": _required(STRING),
        "product_category_name_english": _required(STRING),
    },
}

SCHEMAS: Final[dict[str, pa.DataFrameSchema]] = {
    table: _schema(table, PRIMARY_KEYS[table], columns) for table, columns in _COLUMNS.items()
}

# Mismas columnas que las reviews más los días de espera (ADR 0020). Se valida
# aparte para que una review con `order_id` nulo no espere en silencio al plazo.
PENDING_SCHEMA: Final = _schema(
    PENDING_REVIEWS,
    PRIMARY_KEYS["order_reviews"],
    {**_REVIEW_COLUMNS, PENDING_DAYS: _required(INTEGER, NON_NEGATIVE)},
)
