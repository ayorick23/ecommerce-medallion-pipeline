import polars as pl
import pytest

from medallion.silver.reference import (
    BRAZIL_LAT,
    BRAZIL_LNG,
    UNCATEGORIZED,
    build_geolocation_agg,
    build_products,
    build_reference,
)

TRANSLATION = pl.DataFrame(
    {
        "product_category_name": ["perfumaria", "automotivo"],
        "product_category_name_english": ["perfumery", "auto"],
    }
)

MEASURES = {
    "product_name_length": 40,
    "product_description_length": 300,
    "product_photos_qty": 1,
    "product_weight_g": 500,
    "product_length_cm": 20,
    "product_height_cm": 10,
    "product_width_cm": 15,
}


def _products(categories: dict[str, str | None]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "product_id": list(categories),
            "product_category_name": list(categories.values()),
            **{name: [value] * len(categories) for name, value in MEASURES.items()},
        },
        schema_overrides={"product_category_name": pl.String},
    )


def _geo(rows: list[tuple[str, float, float]]) -> pl.DataFrame:
    return pl.DataFrame(
        rows,
        schema=["geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng"],
        orient="row",
    )


def test_products_get_their_english_category() -> None:
    products = build_products(_products({"p2": "automotivo", "p1": "perfumaria"}), TRANSLATION)

    assert products.select("product_id", "product_category_name_english").rows() == [
        ("p1", "perfumery"),
        ("p2", "auto"),
    ]


def test_products_without_category_are_normalized() -> None:
    products = build_products(_products({"p1": None}), TRANSLATION)

    assert products.select("product_category_name", "product_category_name_english").row(0) == (
        UNCATEGORIZED,
        UNCATEGORIZED,
    )


def test_categories_without_translation_fall_back_to_portuguese() -> None:
    products = build_products(_products({"p1": "pc_gamer"}), TRANSLATION)

    assert products["product_category_name_english"].to_list() == ["pc_gamer"]


def test_products_keep_every_row_and_the_contract_column_order() -> None:
    source = _products({"p1": "perfumaria", "p2": None, "p3": "pc_gamer"})

    products = build_products(source, TRANSLATION)

    assert products.height == source.height
    assert products.columns == [
        "product_id",
        "product_category_name",
        "product_category_name_english",
        *MEASURES,
    ]


def test_geolocation_is_averaged_per_zip_code_and_sorted() -> None:
    agg = build_geolocation_agg(
        _geo([("02000", -23.0, -46.0), ("01000", -22.0, -45.0), ("02000", -25.0, -48.0)])
    )

    assert agg.rows() == [("01000", -22.0, -45.0), ("02000", -24.0, -47.0)]


def test_coordinates_outside_brazil_are_dropped_before_averaging() -> None:
    agg = build_geolocation_agg(
        _geo([("01000", -23.0, -46.0), ("01000", 42.0, -8.0), ("01000", -23.0, 10.0)])
    )

    assert agg.rows() == [("01000", -23.0, -46.0)]


def test_zip_codes_without_any_valid_coordinate_are_left_out() -> None:
    agg = build_geolocation_agg(_geo([("01000", -23.0, -46.0), ("99999", 42.0, -8.0)]))

    assert agg["geolocation_zip_code_prefix"].to_list() == ["01000"]


@pytest.mark.parametrize(
    ("lat", "lng"),
    [(BRAZIL_LAT[0], BRAZIL_LNG[0]), (BRAZIL_LAT[1], BRAZIL_LNG[1])],
    ids=["extremo-suroeste", "extremo-noreste"],
)
def test_brazil_extremes_are_inside(lat: float, lng: float) -> None:
    assert build_geolocation_agg(_geo([("01000", lat, lng)])).height == 1


def test_build_reference_returns_the_five_tables_sorted_by_pk() -> None:
    parsed = {
        "customers": pl.DataFrame({"customer_id": ["c2", "c1"]}),
        "products": _products({"p2": "automotivo", "p1": None}),
        "sellers": pl.DataFrame({"seller_id": ["s2", "s1"]}),
        "geolocation": _geo([("02000", -23.0, -46.0), ("01000", -22.0, -45.0)]),
        "category_translation": TRANSLATION,
    }

    silver = build_reference(parsed)

    assert list(silver) == [
        "customers",
        "products",
        "sellers",
        "geolocation_agg",
        "category_translation",
    ]
    assert silver["customers"]["customer_id"].to_list() == ["c1", "c2"]
    assert silver["products"]["product_id"].to_list() == ["p1", "p2"]
    assert silver["sellers"]["seller_id"].to_list() == ["s1", "s2"]
    assert silver["category_translation"]["product_category_name"].to_list() == [
        "automotivo",
        "perfumaria",
    ]
