"""Tablas de referencia de Silver (docs/schemas.md, sección 3; ADR 0003, 0025).

Son snapshots completos sin dimensión temporal: no se enmascaran a la fecha D.
Cada tabla sale ordenada por su PK, para que el contenido de una corrida no
dependa del orden en que Polars procese los datos (idempotencia).
"""

from collections.abc import Mapping
from typing import Final

import polars as pl

UNCATEGORIZED: Final = "sem_categoria"

# Puntos extremos de Brasil (ADR 0025): un hecho geográfico, no un parámetro.
BRAZIL_LAT: Final = (-33.75, 5.27)  # Arroio Chuí, Monte Caburaí
BRAZIL_LNG: Final = (-73.99, -34.79)  # Serra do Divisor, Ponta do Seixas

_MEASURES: Final = (
    "product_name_length",
    "product_description_length",
    "product_photos_qty",
    "product_weight_g",
    "product_length_cm",
    "product_height_cm",
    "product_width_cm",
)


def build_products(products: pl.DataFrame, translation: pl.DataFrame) -> pl.DataFrame:
    """Productos con la categoría normalizada y su nombre en inglés (ADR 0003).

    Sin categoría → ``sem_categoria``; sin traducción → el nombre en portugués.
    """
    category = pl.col("product_category_name")
    return (
        products.with_columns(category.fill_null(UNCATEGORIZED))
        .join(
            translation.select("product_category_name", "product_category_name_english"),
            on="product_category_name",
            how="left",
        )
        .with_columns(pl.col("product_category_name_english").fill_null(category))
        .select(
            "product_id",
            "product_category_name",
            "product_category_name_english",
            *_MEASURES,
        )
        .sort("product_id")
    )


def build_geolocation_agg(geolocation: pl.DataFrame) -> pl.DataFrame:
    """Una fila por código postal con la latitud y longitud promedio.

    Antes de promediar se descartan las coordenadas fuera de Brasil (ADR
    0025); un código sin ninguna coordenada válida no queda en la tabla.
    """
    lat, lng = pl.col("geolocation_lat"), pl.col("geolocation_lng")
    return (
        geolocation.filter(lat.is_between(*BRAZIL_LAT), lng.is_between(*BRAZIL_LNG))
        .group_by("geolocation_zip_code_prefix")
        .agg(lat.mean(), lng.mean())
        .sort("geolocation_zip_code_prefix")
    )


def build_reference(parsed: Mapping[str, pl.DataFrame]) -> dict[str, pl.DataFrame]:
    """Las 5 tablas de referencia de Silver, a partir de las tablas de Bronze ya tipadas."""
    return {
        "customers": parsed["customers"].sort("customer_id"),
        "products": build_products(parsed["products"], parsed["category_translation"]),
        "sellers": parsed["sellers"].sort("seller_id"),
        "geolocation_agg": build_geolocation_agg(parsed["geolocation"]),
        "category_translation": parsed["category_translation"].sort("product_category_name"),
    }
